/*
 * Copyright (c) 2013-2019 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
 */
package com.snowplowanalytics.snowplow.collectors.scalastream
package sinks

import java.nio.ByteBuffer
import java.util.concurrent.ScheduledExecutorService

import scala.collection.JavaConverters._
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Success, Failure}

import cats.syntax.either._
import com.amazonaws.auth._
import com.amazonaws.services.kinesis.model._
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.services.kinesis.{AmazonKinesis, AmazonKinesisClientBuilder}

import model._

/** KinesisSink companion object with factory method */
object KinesisSink {

  @volatile var shuttingDown = false

  /**
   * Create a KinesisSink and schedule a task to flush its EventStorage
   * Exists so that no threads can get a reference to the KinesisSink
   * during its construction
   * TODO: rm scalaz \/ once 2.12
   */
  def createAndInitialize(
    kinesisConfig: Kinesis,
    bufferConfig: BufferConfig,
    streamName: String,
    executorService: ScheduledExecutorService
  ): Either[Throwable, KinesisSink] = {
    val client = for {
      provider <- getProvider(kinesisConfig.aws)
      client = createKinesisClient(provider, kinesisConfig.endpoint, kinesisConfig.region)
      exists <-
        if (streamExists(client, streamName)) true.asRight
        else new IllegalArgumentException(s"Kinesis stream $streamName doesn't exist").asLeft
    } yield client

    client.map { c =>
      val ks = new KinesisSink(c, kinesisConfig, bufferConfig, streamName, executorService)
      ks.scheduleFlush()

      // When the application is shut down, stop accepting incoming requests
      // and send all stored events
      Runtime.getRuntime.addShutdownHook(new Thread {
        override def run(): Unit = {
          shuttingDown = true
          ks.EventStorage.flush()
          ks.shutdown()
        }
      })
      ks
    }
  }

  /** Create an aws credentials provider through env variables and iam. */
  private def getProvider(awsConfig: AWSConfig): Either[Throwable, AWSCredentialsProvider] = {
    def isDefault(key: String): Boolean = key == "default"
    def isIam(key: String): Boolean = key == "iam"
    def isEnv(key: String): Boolean = key == "env"

    ((awsConfig.accessKey, awsConfig.secretKey) match {
      case (a, s) if isDefault(a) && isDefault(s) =>
        new DefaultAWSCredentialsProviderChain().asRight
      case (a, s) if isDefault(a) || isDefault(s) =>
        "accessKey and secretKey must both be set to 'default' or neither".asLeft
      case (a, s) if isIam(a) && isIam(s) =>
        InstanceProfileCredentialsProvider.getInstance().asRight
      case (a, s) if isIam(a) && isIam(s) =>
        "accessKey and secretKey must both be set to 'iam' or neither".asLeft
      case (a, s) if isEnv(a) && isEnv(s) =>
        new EnvironmentVariableCredentialsProvider().asRight
      case (a, s) if isEnv(a) || isEnv(s) =>
        "accessKey and secretKey must both be set to 'env' or neither".asLeft
      case _ => new AWSStaticCredentialsProvider(
        new BasicAWSCredentials(awsConfig.accessKey, awsConfig.secretKey)).asRight
    }).leftMap(new IllegalArgumentException(_))
  }

  /**
   * Creates a new Kinesis client.
   * @param provider aws credentials provider
   * @param endpoint kinesis endpoint where the stream resides
   * @param region aws region where the stream resides
   * @return the initialized AmazonKinesisClient
   */
  private def createKinesisClient(
    provider: AWSCredentialsProvider,
    endpoint: String,
    region: String
  ): AmazonKinesis =
    AmazonKinesisClientBuilder
      .standard()
      .withCredentials(provider)
      .withEndpointConfiguration(new EndpointConfiguration(endpoint, region))
      .build()

  /**
   * Check whether a Kinesis stream exists
   *
   * @param name Name of the stream
   * @return Whether the stream exists
   */
  private def streamExists(client: AmazonKinesis, name: String): Boolean = try {
    val describeStreamResult = client.describeStream(name)
    val status = describeStreamResult.getStreamDescription.getStreamStatus
    status == "ACTIVE" || status == "UPDATING"
  } catch {
    case rnfe: ResourceNotFoundException => false
  }
}

/**
 * Kinesis Sink for the Scala collector.
 */
class KinesisSink private (
  client: AmazonKinesis,
  kinesisConfig: Kinesis,
  bufferConfig: BufferConfig,
  streamName: String,
  executorService: ScheduledExecutorService
) extends Sink {
  // Records must not exceed MaxBytes - 1MB
  val MaxBytes = 1000000
  val BackoffTime = 3000L

  val ByteThreshold = bufferConfig.byteLimit
  val RecordThreshold = bufferConfig.recordLimit
  val TimeThreshold = bufferConfig.timeLimit

  private val maxBackoff = kinesisConfig.backoffPolicy.maxBackoff
  private val minBackoff = kinesisConfig.backoffPolicy.minBackoff
  private val randomGenerator = new java.util.Random()

  log.info("Creating thread pool of size " + kinesisConfig.threadPoolSize)

  implicit lazy val ec = concurrent.ExecutionContext.fromExecutorService(executorService)

  /**
   * Recursively schedule a task to send everthing in EventStorage
   * Even if the incoming event flow dries up, all stored events will eventually get sent
   *
   * Whenever TimeThreshold milliseconds have passed since the last call to flush, call flush.
   *
   * @param interval When to schedule the next flush
   */
  def scheduleFlush(interval: Long = TimeThreshold): Unit = {
    executorService.schedule(new Thread {
      override def run(): Unit = {
        val lastFlushed = EventStorage.getLastFlushTime()
        val currentTime = System.currentTimeMillis()
        if (currentTime - lastFlushed >= TimeThreshold) {
          EventStorage.flush()
          scheduleFlush(TimeThreshold)
        } else {
          scheduleFlush(TimeThreshold + lastFlushed - currentTime)
        }
      }
    }, interval, MILLISECONDS)
  }

  object EventStorage {
    private var storedEvents = List[(ByteBuffer, String)]()
    private var byteCount = 0L
    @volatile private var lastFlushedTime = 0L

    def store(event: Array[Byte], key: String): Unit = {
      val eventBytes = ByteBuffer.wrap(event)
      val eventSize = eventBytes.capacity
      if (eventSize >= MaxBytes) {
        log.error(s"Record of size $eventSize bytes is too large - must be less than $MaxBytes bytes")
      } else {
        synchronized {
          storedEvents = (eventBytes, key) :: storedEvents
          byteCount += eventSize
          if (storedEvents.size >= RecordThreshold || byteCount >= ByteThreshold) {
            flush()
          }
        }
      }
    }

    def flush(): Unit = {
      val eventsToSend = synchronized {
        val evts = storedEvents.reverse
        storedEvents = Nil
        byteCount = 0
        evts
      }
      lastFlushedTime = System.currentTimeMillis()
      sendBatch(eventsToSend)
    }

    def getLastFlushTime(): Long = lastFlushedTime
  }

  def storeRawEvents(events: List[Array[Byte]], key: String): List[Array[Byte]] = {
    events.foreach(e => EventStorage.store(e, key))
    Nil
  }

  def scheduleBatch(batch: List[(ByteBuffer, String)], lastBackoff: Long = minBackoff): Unit = {
    val nextBackoff = getNextBackoff(lastBackoff)
    executorService.schedule(new Thread {
      override def run(): Unit = {
        sendBatch(batch, nextBackoff)
      }
    }, lastBackoff, MILLISECONDS)
  }

  // TODO: limit max retries?
  def sendBatch(batch: List[(ByteBuffer, String)], nextBackoff: Long = minBackoff): Unit = {
    if (batch.size > 0) {
      log.info(s"Writing ${batch.size} Thrift records to Kinesis stream ${streamName}")
      val putData = for {
        p <- multiPut(streamName, batch)
      } yield p

      putData onComplete {
        case Success(s) => {
          val results = s.getRecords.asScala.toList
          val failurePairs = batch zip results filter { _._2.getErrorMessage != null }
          log.info(s"Successfully wrote ${batch.size-failurePairs.size} out of ${batch.size} records")
          if (failurePairs.size > 0) {
            failurePairs.foreach(f => log.error(s"Record failed with error code [${f._2.getErrorCode}] and message [${f._2.getErrorMessage}]"))
            log.error(s"Retrying all failed records in $nextBackoff milliseconds...")
            val failures = failurePairs.map(_._1)
            scheduleBatch(failures, nextBackoff)
          }
        }
        case Failure(f) => {
          log.error("Writing failed.", f)
          log.error(s"Retrying in $nextBackoff milliseconds...")
          scheduleBatch(batch, nextBackoff)
        }
      }
    }
  }

  private def multiPut(name: String, batch: List[(ByteBuffer, String)]): Future[PutRecordsResult] =
    Future {
      val putRecordsRequest = {
        val prr = new PutRecordsRequest()
        prr.setStreamName(name)
        val putRecordsRequestEntryList = batch.map { case (b, s) =>
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

  /**
   * How long to wait before sending the next request
   *
   * @param lastBackoff The previous backoff time
   * @return Minimum of maxBackoff and a random number between minBackoff and three times lastBackoff
   */
  private def getNextBackoff(lastBackoff: Long): Long = (minBackoff + randomGenerator.nextDouble() * (lastBackoff * 3 - minBackoff)).toLong.min(maxBackoff)

  def shutdown(): Unit = {
    executorService.shutdown()
    executorService.awaitTermination(10000, MILLISECONDS)
  }
}
