/*
 * Copyright (c) 2013-2014 Snowplow Analytics Ltd.
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
package kinesis
package sinks

// Java
import java.nio.ByteBuffer

// Amazon
import com.amazonaws.services.kinesis.model.ResourceNotFoundException
import com.amazonaws.AmazonServiceException
import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.services.kinesis.AmazonKinesisClient
import com.amazonaws.services.kinesis.AmazonKinesis
import com.amazonaws.regions._

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

/**
 * Kinesis Sink for Scala enrichment
 */
class KinesisSink(provider: AWSCredentialsProvider,
    config: KinesisEnrichConfig, inputType: InputType.InputType) extends ISink {
  private lazy val log = LoggerFactory.getLogger(getClass())
  import log.{error, debug, info, trace}
  
  // explicitly create a client so we can configure the end point
  val client = new AmazonKinesisClient(provider)
  client.setEndpoint(config.streamEndpoint)
  
  // Create a Kinesis client for stream interactions.
  private implicit val kinesis = Client.fromClient(client)

  // The output stream for enriched events.
  private val enrichedStream = createAndLoadStream()

  // Checks if a stream exists.
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
   * Creates a new stream if one doesn't exist
   */
  def createAndLoadStream(timeout: Int = 60): Stream = {
    val (name, size) = inputType match {
      case InputType.Good => (config.enrichedOutStream, config.enrichedOutStreamShards)
      case InputType.Bad => (config.badOutStream, config.badOutStreamShards)
    }

    if (streamExists(name)) {
      Kinesis.stream(name)
    } else {
      info(s"Creating stream $name of size $size")
      val createStream = for {
        s <- Kinesis.streams.create(name)
      } yield s

      try {
        val stream = Await.result(createStream, Duration(timeout, SECONDS))

        info(s"Successfully created stream $name. Waiting until it's active")
        Await.result(stream.waitActive.retrying(timeout),
          Duration(timeout, SECONDS))

        info(s"Stream $name active")

        stream
      } catch {
        case _: TimeoutException =>
          throw new RuntimeException("Error: Timed out")
      }
    }
  }

  // TODO: make these configurable
  // Note that it is possible to send
  val ByteLimit = 4000000
  val RecordLimit = 500
  val TimeLimit = 60000 // 1 minute TODO implement

  // TODO create an object to hold these
  var stored = List[(ByteBuffer, String)]()
  var storedBytes = 0

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
    val wrappedEvents = events.map(e => ByteBuffer.wrap(e._1.getBytes) -> e._2)
    val totalBytes = wrappedEvents.foldLeft(0)(_ + _._1.capacity)
    stored = stored ++ wrappedEvents
    storedBytes += totalBytes
    stored.size >= RecordLimit || storedBytes >= ByteLimit // TODO: decide on this value
  }

  /**
   * Blocking method to send all stored records to Kinesis
   * Splits the stored records into smaller batches (by byte size or record number) if necessary
   */
  def flush() {

    import scala.annotation.tailrec
    @tailrec
    def batchAndSend(ls: List[(ByteBuffer, String)], currentLength: Int, currentBytes: Int, batch: List[(ByteBuffer, String)]): List[(ByteBuffer, String)] =
      ls match {
        case head :: tail => if (currentBytes + head._1.capacity > ByteLimit || currentLength == RecordLimit) {
          sendBatch(batch)
          batchAndSend(head :: tail, 0, 0, Nil)
        } else {
          // TODO This reverses the order of the events in the batch
          batchAndSend(tail, currentLength + 1, currentBytes + head._1.capacity, head :: batch)
        }
        case Nil => {
          sendBatch(batch)
          Nil
        }
      }

    batchAndSend(stored, 0, 0, Nil)

    stored = List[(ByteBuffer, String)]()
    storedBytes = 0
  }

  /**
   * Send a single batch of events in one blocking PutRecords API call
   *
   * @param batch Events to send
   */
  def sendBatch(batch: List[(ByteBuffer, String)]) {
    if (!batch.isEmpty) {
      val putData = for {
        p <- enrichedStream.multiPut(batch)
      } yield p

      putData onComplete {
        case Success(result) => {
          info(s"Writing successful")
          info(s"  + ShardIds: ${result.shardIds}")
          info(s"  + SequenceNumber: ${result.sequenceNumber}")
        }
        case Failure(f) => {
          error(s"Writing failed.")
          error(s"  + " + f.getMessage)
        }
      }

      Await.result(putData, 10.seconds)
    }
  }
}
