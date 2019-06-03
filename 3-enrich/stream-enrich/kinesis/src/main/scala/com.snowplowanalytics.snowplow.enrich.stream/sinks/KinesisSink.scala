/*
 * Copyright (c) 2013-2019 Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache
 * License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at
 * http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied.
 *
 * See the Apache License Version 2.0 for the specific language
 * governing permissions and limitations there under.
 */
package com.snowplowanalytics.snowplow
package enrich
package stream
package sinks

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets.UTF_8

import scala.collection.JavaConverters._
import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.util.Try
import scala.util.control.NonFatal

import com.amazonaws.services.kinesis.model._
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.services.kinesis.{AmazonKinesis, AmazonKinesisClientBuilder}
import scalaz._
import Scalaz._

import model._
import scalatracker.Tracker

/** KinesisSink companion object with factory method */
object KinesisSink {
  def validate(kinesisConfig: Kinesis, streamName: String): \/[String, Unit] =
    for {
      provider <- KinesisEnrich.getProvider(kinesisConfig.aws)
      endpointConfiguration = new EndpointConfiguration(
        kinesisConfig.streamEndpoint,
        kinesisConfig.region
      )
      client = AmazonKinesisClientBuilder
        .standard()
        .withCredentials(provider)
        .withEndpointConfiguration(endpointConfiguration)
        .build()
      _ <- streamExists(client, streamName)
        .leftMap(_.getMessage)
        .ensure(s"Kinesis stream $streamName doesn't exist")(_ == true)
    } yield ()

  /**
   * Check whether a Kinesis stream exists
   * @param name Name of the stream
   * @return Whether the stream exists
   */
  private def streamExists(client: AmazonKinesis, name: String): \/[Throwable, Boolean] = {
    val existsTry = Try {
      val describeStreamResult = client.describeStream(name)
      val status = describeStreamResult.getStreamDescription.getStreamStatus
      status == "ACTIVE" || status == "UPDATING"
    }
    utils.toEither(existsTry)
  }
}

/** Kinesis Sink for Scala enrichment */
class KinesisSink(
  client: AmazonKinesis,
  backoffPolicy: KinesisBackoffPolicyConfig,
  buffer: BufferConfig,
  streamName: String,
  tracker: Option[Tracker]
) extends Sink {

  /** Kinesis records must not exceed 1MB */
  private val MaxBytes = 1000000L

  private val maxBackoff = backoffPolicy.maxBackoff
  private val minBackoff = backoffPolicy.minBackoff
  private val randomGenerator = new java.util.Random()

  val ByteThreshold = buffer.byteLimit
  val RecordThreshold = buffer.recordLimit
  val TimeThreshold = buffer.timeLimit
  var nextRequestTime = 0L

  /**
   * Object to store events while waiting for the ByteThreshold, RecordThreshold, or TimeThreshold to be reached
   */
  object EventStorage {
    // Each complete batch is the contents of a single PutRecords API call
    var completeBatches = List[List[(ByteBuffer, String)]]()
    // The batch currently under constructon
    var currentBatch = List[(ByteBuffer, String)]()
    // Length of the current batch
    var eventCount = 0
    // Size in bytes of the current batch
    var byteCount = 0

    /**
     * Finish work on the current batch and create a new one.
     */
    def sealBatch(): Unit = {
      completeBatches = currentBatch :: completeBatches
      eventCount = 0
      byteCount = 0
      currentBatch = Nil
    }

    /**
     * Add a new event to the current batch.
     * If this would take the current batch above ByteThreshold bytes,
     * first seal the current batch.
     * If this takes the current batch up to RecordThreshold records,
     * seal the current batch and make a new batch.
     *
     * @param event New event
     */
    def addEvent(event: (ByteBuffer, String)): Unit = {
      val newBytes = event._1.capacity

      if (newBytes >= MaxBytes) {
        val original = new String(event._1.array, UTF_8)
        log.error(s"Dropping record with size $newBytes bytes: [$original]")
      } else {

        if (byteCount + newBytes >= ByteThreshold) {
          sealBatch()
        }

        byteCount += newBytes

        eventCount += 1
        currentBatch = event :: currentBatch

        if (eventCount == RecordThreshold) {
          sealBatch()
        }
      }
    }

    /**
     * Reset everything.
     */
    def clear(): Unit = {
      completeBatches = Nil
      currentBatch = Nil
      eventCount = 0
      byteCount = 0
    }
  }

  /**
   * Side-effecting function to store the EnrichedEvent
   * to the given output stream.
   *
   * EnrichedEvent takes the form of a tab-delimited
   * String until such time as https://github.com/snowplow/snowplow/issues/211
   * is implemented.
   *
   * This method blocks until the request has finished.
   *
   * @param events List of events together with their partition keys
   * @return whether to send the stored events to Kinesis
   */
  override def storeEnrichedEvents(events: List[(String, String)]): Boolean = {
    val wrappedEvents = events.map(e => ByteBuffer.wrap(e._1.getBytes(UTF_8)) -> e._2)
    wrappedEvents.foreach(EventStorage.addEvent(_))
    if (!EventStorage.currentBatch.isEmpty && System.currentTimeMillis() > nextRequestTime) {
      nextRequestTime = System.currentTimeMillis() + TimeThreshold
      true
    } else {
      !EventStorage.completeBatches.isEmpty
    }
  }

  /**
   * Blocking method to send all stored records to Kinesis
   * Splits the stored records into smaller batches (by byte size or record number) if necessary
   */
  override def flush(): Unit = {
    EventStorage.sealBatch()
    // Send events in the order they were received
    EventStorage.completeBatches.reverse.foreach(b => sendBatch(b.reverse))
    EventStorage.clear()
  }

  /**
   * Send a single batch of events in one blocking PutRecords API call
   * Loop until all records have been sent successfully
   * Cannot be made tail recursive (http://stackoverflow.com/questions/8233089/why-wont-scala-optimize-tail-call-with-try-catch)
   *
   * @param batch Events to send
   */
  def sendBatch(batch: List[(ByteBuffer, String)]): Unit =
    if (!batch.isEmpty) {
      log.info(s"Writing ${batch.size} records to Kinesis stream $streamName")
      var unsentRecords = batch
      var backoffTime = minBackoff
      var sentBatchSuccessfully = false
      var attemptNumber = 0
      while (!sentBatchSuccessfully) {
        attemptNumber += 1

        val putData = for {
          p <- multiPut(streamName, unsentRecords)
        } yield p

        try {
          val results = Await.result(putData, 10.seconds).getRecords.asScala.toList
          val failurePairs = unsentRecords zip results filter { _._2.getErrorMessage != null }
          log.info(
            s"Successfully wrote ${unsentRecords.size - failurePairs.size} out of ${unsentRecords.size} records"
          )
          if (failurePairs.nonEmpty) {
            val (failedRecords, failedResults) = failurePairs.unzip
            unsentRecords = failedRecords
            logErrorsSummary(getErrorsSummary(failedResults))
            backoffTime = getNextBackoff(backoffTime)
            log.error(s"Retrying all failed records in $backoffTime milliseconds...")

            val err = s"Failed to send ${failurePairs.size} events"
            val putSize: Long = unsentRecords.foldLeft(0L)((a, b) => a + b._1.capacity)

            tracker match {
              case Some(t) =>
                SnowplowTracking.sendFailureEvent(
                  t,
                  "PUT Failure",
                  err,
                  streamName,
                  "snowplow-stream-enrich",
                  attemptNumber.toLong,
                  putSize
                )
              case _ => None
            }

            Thread.sleep(backoffTime)
          } else {
            sentBatchSuccessfully = true
          }
        } catch {
          case NonFatal(f) =>
            backoffTime = getNextBackoff(backoffTime)
            log.error(s"Writing failed.", f)
            log.error(s"  + Retrying in $backoffTime milliseconds...")

            val putSize: Long = unsentRecords.foldLeft(0L)((a, b) => a + b._1.capacity)

            tracker match {
              case Some(t) =>
                SnowplowTracking.sendFailureEvent(
                  t,
                  "PUT Failure",
                  f.toString,
                  streamName,
                  "snowplow-stream-enrich",
                  attemptNumber.toLong,
                  putSize
                )
              case _ => None
            }

            Thread.sleep(backoffTime)
        }
      }
    }

  private def multiPut(name: String, batch: List[(ByteBuffer, String)]): Future[PutRecordsResult] =
    Future {
      val putRecordsRequest = {
        val prr = new PutRecordsRequest()
        prr.setStreamName(name)
        val putRecordsRequestEntryList = batch.map {
          case (b, s) =>
            val prre = new PutRecordsRequestEntry()
            prre.setPartitionKey(s)
            prre.setData(b)
            prre
        }
        prr.setRecords(putRecordsRequestEntryList.asJava)
        prr
      }
      client.putRecords(putRecordsRequest)
    }

  private[sinks] def getErrorsSummary(
    badResponses: List[PutRecordsResultEntry]
  ): Map[String, (Long, String)] =
    badResponses.foldLeft(Map[String, (Long, String)]())(
      (counts, r) =>
        if (counts.contains(r.getErrorCode)) {
          counts + (r.getErrorCode -> (counts(r.getErrorCode)._1 + 1 -> r.getErrorMessage))
        } else {
          counts + (r.getErrorCode -> ((1, r.getErrorMessage)))
        }
    )

  private[sinks] def logErrorsSummary(errorsSummary: Map[String, (Long, String)]): Unit =
    for ((errorCode, (count, sampleMessage)) <- errorsSummary) {
      log.error(
        s"$count records failed with error code ${errorCode}. Example error message: ${sampleMessage}"
      )
    }

  /**
   * How long to wait before sending the next request
   *
   * @param lastBackoff The previous backoff time
   * @return Minimum of maxBackoff and a random number between minBackoff and three times lastBackoff
   */
  private def getNextBackoff(lastBackoff: Long): Long = {
    val offset: Long = (randomGenerator.nextDouble() * (lastBackoff * 3 - minBackoff)).toLong
    val sum: Long = minBackoff + offset
    sum min maxBackoff
  }
}
