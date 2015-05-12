/*
 * Copyright (c) 2013-2014 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow
package collectors
package scalastream
package sinks

// Java
import java.nio.ByteBuffer
import java.util.concurrent.ScheduledThreadPoolExecutor

// Amazon
import com.amazonaws.services.kinesis.model.ResourceNotFoundException
import com.amazonaws.AmazonServiceException
import com.amazonaws.auth.{
  BasicAWSCredentials,
  ClasspathPropertiesFileCredentialsProvider
}
import com.amazonaws.services.kinesis.AmazonKinesisClient

// Scalazon (for Kinesis interaction)
import io.github.cloudify.scala.aws.kinesis.Client
import io.github.cloudify.scala.aws.kinesis.Client.ImplicitExecution._
import io.github.cloudify.scala.aws.kinesis.Definitions.{
  Stream,
  PutResult,
  Record
}
import io.github.cloudify.scala.aws.kinesis.KinesisDsl._
import io.github.cloudify.scala.aws.auth.CredentialsProvider.InstanceProfile

// Config
import com.typesafe.config.Config

// Concurrent libraries
import scala.concurrent.{Future,Await,TimeoutException}
import scala.concurrent.duration._

// Logging
import org.slf4j.LoggerFactory

// Scala
import scala.util.{Success, Failure}
import scala.collection.JavaConverters._

// Snowplow
import scalastream._
import CollectorPayload.thrift.model1.CollectorPayload

/**
 * KinesisSink companion object with factory method
 */
object KinesisSink {

  @volatile var shuttingDown = false

  /**
   * Create a KinesisSink and schedule a task to flush its EventStorage
   * Exists so that no threads can get a reference to the KinesisSink
   * during its construction
   *
   * @param config
   */
  def createAndInitialize(config: CollectorConfig): KinesisSink = {
    val ks = new KinesisSink(config)
    ks.scheduleFlush()

    // When the application is shut down, stop accepting incoming requests
    // and send all stored events
    Runtime.getRuntime.addShutdownHook(new Thread {
      override def run() {
        shuttingDown = true
        ks.EventStorage.flush()
        ks.executorService.shutdown()
        ks.executorService.awaitTermination(10000, MILLISECONDS)
      }
    })
    ks
  }
}

/**
 * Kinesis Sink for the Scala collector.
 */
class KinesisSink private (config: CollectorConfig) extends AbstractSink {

  import log.{error, debug, info, trace}

  // Records must not exceed MaxBytes - 1MB
  val MaxBytes = 1000000L

  val BackoffTime = 3000L

  val ByteThreshold = config.byteLimit
  val RecordThreshold = config.recordLimit
  val TimeThreshold = config.timeLimit

  private val maxBackoff = config.maxBackoff
  private val minBackoff = config.minBackoff
  private val randomGenerator = new java.util.Random()

  info("Creating thread pool of size " + config.threadpoolSize)

  val executorService = new ScheduledThreadPoolExecutor(config.threadpoolSize)
  implicit lazy val ec = concurrent.ExecutionContext.fromExecutorService(executorService)

  /**
   * Recursively schedule a task to send everthing in EventStorage
   * Even if the incoming event flow dries up, all stored events will eventually get sent
   *
   * Whenever TimeThreshold milliseconds have passed since the last call to flush, call flush.
   *
   * @param interval When to schedule the next flush
   */
  def scheduleFlush(interval: Long = TimeThreshold) {
    executorService.schedule(new Thread {
      override def run() {
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

  // Create a Kinesis client for stream interactions.
  private implicit val kinesis = createKinesisClient

  // The output stream for enriched events.
  private val enrichedStream = loadStream()

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
      info(s"Stream $name doesn't exist or is not active")
    }

    exists
  }

  /**
   * Loads a Kinesis stream if it exists
   *
   * @return The stream
   */
  def loadStream(): Stream = {
    val name = config.streamName

    if (streamExists(name)) {
      Kinesis.stream(name)
    } else {
      throw new RuntimeException(s"Cannot write because stream $name doesn't exist or is not active")
    }
  }

  /**
   * Creates a new Kinesis client from provided AWS access key and secret
   * key. If both are set to "cpf", then authenticate using the classpath
   * properties file.
   *
   * @return the initialized AmazonKinesisClient
   */
  private def createKinesisClient: Client = {
    val accessKey = config.awsAccessKey
    val secretKey = config.awsSecretKey
    val client = if (isCpf(accessKey) && isCpf(secretKey)) {
      new AmazonKinesisClient(new ClasspathPropertiesFileCredentialsProvider())
    } else if (isCpf(accessKey) || isCpf(secretKey)) {
      throw new RuntimeException("access-key and secret-key must both be set to 'cpf', or neither of them")
    } else if (isIam(accessKey) && isIam(secretKey)) {
      new AmazonKinesisClient(InstanceProfile)
    } else if (isIam(accessKey) || isIam(secretKey)) {
      throw new RuntimeException("access-key and secret-key must both be set to 'iam', or neither of them")
    } else if (isEnv(accessKey) && isEnv(secretKey)) {
      new AmazonKinesisClient()
    } else if (isEnv(accessKey) || isEnv(secretKey)) {
      throw new RuntimeException("access-key and secret-key must both be set to 'env', or neither of them")
    } else {
      new AmazonKinesisClient(new BasicAWSCredentials(accessKey, secretKey))
    }

    client.setEndpoint(config.streamEndpoint)
    Client.fromClient(client)
  }

  // TODO: don't send more than 4.5MB per request
  object EventStorage {
    private var storedEvents = List[(ByteBuffer, String)]()
    private var byteCount = 0L
    @volatile private var lastFlushedTime = 0L

    def store(event: Array[Byte], key: String) = {
      val eventBytes = ByteBuffer.wrap(event)
      val eventSize = eventBytes.capacity
      if (eventSize >= MaxBytes) {
        // TODO: split up large event arrays (see https://github.com/snowplow/snowplow/issues/941)
        error(s"Record of size $eventSize bytes is too large - must be less than $MaxBytes bytes")
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

    def flush() = {
      val eventsToSend = synchronized {
        val evts = storedEvents.reverse
        storedEvents = Nil
        byteCount = 0
        evts
      }
      lastFlushedTime = System.currentTimeMillis()
      sendBatch(eventsToSend)
    }

    def getLastFlushTime() = lastFlushedTime
  }

  def storeRawEvent(event: CollectorPayload, key: String) = {
    splitAndSerializePayload(event) foreach {
      e => EventStorage.store(e, key)
    }
    null
  }

  def scheduleBatch(batch: List[(ByteBuffer, String)], lastBackoff: Long = minBackoff) {
    val nextBackoff = getNextBackoff(lastBackoff)
    executorService.schedule(new Thread {
      override def run() {
        sendBatch(batch, nextBackoff)
      }
    }, lastBackoff, MILLISECONDS)
  }

  // TODO: limit max retries?
  def sendBatch(batch: List[(ByteBuffer, String)], nextBackoff: Long = minBackoff) {
    if (batch.size > 0) {
      info(s"Writing ${batch.size} Thrift records to Kinesis stream ${config.streamName}")
      val putData = for {
        p <- enrichedStream.multiPut(batch)
      } yield p

      putData onComplete {
        case Success(s) => {
          val results = s.result.getRecords.asScala.toList
          val failurePairs = batch zip results filter { _._2.getErrorMessage != null }
          info(s"Successfully wrote ${batch.size-failurePairs.size} out of ${batch.size} records")
          if (failurePairs.size > 0) {
            failurePairs foreach { f => error(s"Record failed with error code [${f._2.getErrorCode}] and message [${f._2.getErrorMessage}]") }
            error("Retrying all failed records in $nextBackoff milliseconds...")
            val failures = failurePairs.map(_._1)
            scheduleBatch(failures, nextBackoff)
          }
        }
        case Failure(f) => {
          error("Writing failed.", f)
          error(s"Retrying in $nextBackoff milliseconds...")
          scheduleBatch(batch, nextBackoff)
        }
      }
    }
  }

  /**
   * How long to wait before sending the next request
   *
   * @param lastBackoff The previous backoff time
   * @return Minimum of maxBackoff and a random number between minBackoff and three times lastBackoff
   */
  private def getNextBackoff(lastBackoff: Long): Long = (minBackoff + randomGenerator.nextDouble() * (lastBackoff * 3 - minBackoff)).toLong.min(maxBackoff)

  /**
   * Is the access/secret key set to the special value "cpf" i.e. use
   * the classpath properties file for credentials.
   *
   * @param key The key to check
   * @return true if key is cpf, false otherwise
   */
  private def isCpf(key: String): Boolean = (key == "cpf")

  /**
   * Is the access/secret key set to the special value "iam" i.e. use
   * the IAM role to get credentials.
   *
   * @param key The key to check
   * @return true if key is iam, false otherwise
   */
  private def isIam(key: String): Boolean = (key == "iam")

  /**
   * Is the access/secret key set to the special value "env" i.e. get
   * the credentials from environment variables
   *
   * @param key The key to check
   * @return true if key is iam, false otherwise
   */
  private def isEnv(key: String): Boolean = (key == "env")
}
