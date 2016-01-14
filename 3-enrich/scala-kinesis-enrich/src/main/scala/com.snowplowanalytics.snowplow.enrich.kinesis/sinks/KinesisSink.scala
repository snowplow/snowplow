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


  var stored = List[(ByteBuffer, String)]()

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
    stored = stored ++ events.map(e => ByteBuffer.wrap(e._1.getBytes) -> e._2)
    stored.size >= 10 // TODO: decide on this value
  }

  /**
   * Blocking method to send all batched records to Kinesis
   */
  def flush() {
    if (stored.size > 0) {

      // At most 500 records per request
      stored.grouped(500) foreach { batch =>

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

      stored = List[(ByteBuffer, String)]()
    }
  }
}
