/*
 * Copyright (c) 2013-2016 Snowplow Analytics Ltd.
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
package com.snowplowanalytics.snowplow.enrich
package stream
package sinks

// Java
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets.UTF_8

// Amazon
import com.amazonaws.services.kinesis.model.ResourceNotFoundException
import com.amazonaws.services.kinesis.model.PutRecordsResultEntry
import com.amazonaws.AmazonServiceException
import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.services.kinesis.AmazonKinesisClient
import com.amazonaws.services.kinesis.AmazonKinesis
import com.amazonaws.regions._

// Scala
import scala.util.control.NonFatal
import scala.collection.JavaConverters._

// Scalazon (for Kinesis interaction)
import io.github.cloudify.scala.aws.kinesis.Client
import io.github.cloudify.scala.aws.kinesis.Client.ImplicitExecution._
import io.github.cloudify.scala.aws.kinesis.Definitions.{
  Stream,
  PutResult,
  Record
}
import io.github.cloudify.scala.aws.kinesis.KinesisDsl._

// Config
import com.typesafe.config.Config

// Concurrent libraries
import scala.concurrent.{Future,Await,TimeoutException}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.util.{Success, Failure}

// Logging
import org.slf4j.LoggerFactory

// Snowplow
import com.snowplowanalytics.snowplow.collectors.thrift._
import common.outputs.EnrichedEvent

// Tracker
import com.snowplowanalytics.snowplow.scalatracker.Tracker

/**
 * Kinesis Sink for Scala enrichment
 */
class KinesisSink(provider: AWSCredentialsProvider,
    config: KinesisEnrichConfig, inputType: InputType.InputType, tracker: Option[Tracker]) extends ISink {
  private lazy val log = LoggerFactory.getLogger(getClass())
  import log.{error, debug, info, trace}

  private val name = inputType match {
    case InputType.Good => config.enrichedOutStream
    case InputType.Bad => config.badOutStream
  }

  private val maxBackoff = config.maxBackoff
  private val minBackoff = config.minBackoff
  private val randomGenerator = new java.util.Random()

  // explicitly create a client so we can configure the end point
  val client = new AmazonKinesisClient(provider)
  client.setEndpoint(config.streamEndpoint)
  
  // Create a Kinesis client for stream interactions.
  private implicit val kinesis = Client.fromClient(client)

  // The output stream for enriched and bad events.
  private val stream = loadStream()

  /**
   * Check whether a Kinesis stream exists
   *
   * @param name Name of the stream
   * @param timeout How long to wait before timing out
   * @return Whether the stream exists
   */
  def streamExists(name: String, timeout: Int = 60): Boolean = {

    val exists: Boolean = try {
      val streamDescribeFuture = for {
        s <- Kinesis.stream(name).describe
      } yield s

      val description = Await.result(streamDescribeFuture, Duration(timeout, SECONDS))
      description.isActive

    } catch {
      case rnfe: ResourceNotFoundException => false
    }

    if (exists) {
      info(s"Stream $name exists and is active")
    } else {
      error(s"Stream $name doesn't exist or is not active")
    }

    exists
  }

  /**
   * Loads a Kinesis stream if it exists
   *
   * @return The stream
   */
  def loadStream(): Stream = {
    if (streamExists(name)) {
      Kinesis.stream(name)
    } else {
      error(s"Cannot write because stream $name does not exist or is not active")
      System.exit(1)
      throw new RuntimeException("System.exit should never fail")
    }
  }

  val ByteThreshold = config.byteLimit
  val RecordThreshold = config.recordLimit
  val TimeThreshold = config.timeLimit
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
    def sealBatch() {
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
    def addEvent(event: (ByteBuffer, String)) {
      val newBytes = event._1.capacity

      if (newBytes >= MaxBytes) {
        val original = new String(event._1.array, UTF_8)
        error(s"Dropping record with size $newBytes bytes: [$original]")
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
    def clear() {
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
  def storeEnrichedEvents(events: List[(String, String)]): Boolean = {
    val wrappedEvents = events.map(e => ByteBuffer.wrap(e._1.getBytes(UTF_8)) -> e._2)
    wrappedEvents.foreach(EventStorage.addEvent(_))

    // Log BadRows
    inputType match {
      case InputType.Good => None
      case InputType.Bad  => events.foreach(e => debug(s"BadRow: ${e._1}"))
    }

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
  def flush() {
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
  def sendBatch(batch: List[(ByteBuffer, String)]) {
    if (!batch.isEmpty) {
      info(s"Writing ${batch.size} records to Kinesis stream $name")
      var unsentRecords = batch
      var backoffTime = minBackoff
      var sentBatchSuccessfully = false
      var attemptNumber = 0
      while (!sentBatchSuccessfully) {
        attemptNumber += 1

        val putData = for {
          p <- stream.multiPut(unsentRecords)
        } yield p

        try {
          val results = Await.result(putData, 10.seconds).result.getRecords.asScala.toList
          val failurePairs = unsentRecords zip results filter { _._2.getErrorMessage != null }
          info(s"Successfully wrote ${unsentRecords.size-failurePairs.size} out of ${unsentRecords.size} records")
          if (failurePairs.nonEmpty) {
            val (failedRecords, failedResults) = failurePairs.unzip
            unsentRecords = failedRecords
            logErrorsSummary(getErrorsSummary(failedResults))
            backoffTime = getNextBackoff(backoffTime)
            error(s"Retrying all failed records in $backoffTime milliseconds...")

            val err = s"Failed to send ${failurePairs.size} events"
            val putSize: Long = unsentRecords.foldLeft(0)((a,b) => a + b._1.capacity)

            tracker match {
              case Some(t) => SnowplowTracking.sendFailureEvent(t, "PUT Failure", err, name, "snowplow-stream-enrich", attemptNumber, putSize)
              case _       => None
            }

            Thread.sleep(backoffTime)
          } else {
            sentBatchSuccessfully = true
          }
        } catch {
          case NonFatal(f) => {
            backoffTime = getNextBackoff(backoffTime)
            error(s"Writing failed.", f)
            error(s"  + Retrying in $backoffTime milliseconds...")

            val putSize: Long = unsentRecords.foldLeft(0)((a,b) => a + b._1.capacity)

            tracker match {
              case Some(t) => SnowplowTracking.sendFailureEvent(t, "PUT Failure", f.toString, name, "snowplow-stream-enrich", attemptNumber, putSize)
              case _       => None
            }

            Thread.sleep(backoffTime)
          }
        }
      }
    }
  }

  private[sinks] def getErrorsSummary(badResponses: List[PutRecordsResultEntry]): Map[String, (Long, String)] = {
    badResponses.foldLeft(Map[String, (Long, String)]())((counts, r) => if (counts.contains(r.getErrorCode)) {
      counts + (r.getErrorCode -> (counts(r.getErrorCode)._1 + 1 -> r.getErrorMessage))
    } else {
      counts + (r.getErrorCode -> (1, r.getErrorMessage))
    })
  }

  private[sinks] def logErrorsSummary(errorsSummary: Map[String, (Long, String)]): Unit = {
    for ((errorCode, (count, sampleMessage)) <- errorsSummary) {
      error(s"$count records failed with error code ${errorCode}. Example error message: ${sampleMessage}")
    }
  }

  /**
   * How long to wait before sending the next request
   *
   * @param lastBackoff The previous backoff time
   * @return Minimum of maxBackoff and a random number between minBackoff and three times lastBackoff
   */
  private def getNextBackoff(lastBackoff: Long): Long = (minBackoff + randomGenerator.nextDouble() * (lastBackoff * 3 - minBackoff)).toLong.min(maxBackoff)
}
