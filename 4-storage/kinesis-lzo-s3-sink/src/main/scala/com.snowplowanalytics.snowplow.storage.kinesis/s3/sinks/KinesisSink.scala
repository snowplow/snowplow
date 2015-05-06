 /*
 * Copyright (c) 2014 Snowplow Analytics Ltd.
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

package com.snowplowanalytics.snowplow.storage.kinesis.s3.sinks

// Java
import java.nio.ByteBuffer

// Scala
import scala.util.Random

// Amazon
import com.amazonaws.services.kinesis.model.ResourceNotFoundException
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

// Concurrent libraries
import scala.concurrent.{Future,Await,TimeoutException}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.util.{Success, Failure}

// Logging
import org.slf4j.LoggerFactory

/**
 * Kinesis Sink
 *
 * @param provider AWSCredentialsProvider
 * @param endpoint Kinesis stream endpoint
 * @param name Kinesis stream name
 * @param shards Number of shards with which to initialize the stream
 * @param config Configuration for the Kinesis stream
 */
class KinesisSink(provider: AWSCredentialsProvider, endpoint: String, name: String, shards: Int)
  extends ISink {

  private lazy val log = LoggerFactory.getLogger(getClass())
  import log.{error, debug, info, trace}

  // Explicitly create a client so we can configure the end point
  val client = new AmazonKinesisClient(provider)
  client.setEndpoint(endpoint)

  // Create a Kinesis client for stream interactions.
  private implicit val kinesis = Client.fromClient(client)

  // The output stream for enriched events.
  // Lazy so that it doesn't get created unless we need to write to it.
  private lazy val enrichedStream = createAndLoadStream()

  /**
   * Checks if a stream exists.
   *
   * @param name Name of the stream to look for
   * @param timeout How long to wait for a description of the stream
   * @return Whether the stream both exists and is active
   */
  // TODO move out into a kinesis helpers library
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
   *
   * @param How long to wait for the stream to be created
   * @return The new stream
   */
  // TODO move out into a kinesis helpers library
  def createAndLoadStream(timeout: Int = 60): Stream = {

    if (streamExists(name)) {
      Kinesis.stream(name)
    } else {
      info(s"Creating stream $name of size $shards")
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

  /**
   * Write a record to the Kinesis stream
   *
   * @param output The string record to write
   * @param key A hash of the key determines to which shard the
   *            record is assigned. Defaults to a random string.
   * @param good Unused parameter which exists to extend ISink
   */
  def store(output: String, key: Option[String], good: Boolean) {
    val putData = for {
      p <- enrichedStream.put(
        ByteBuffer.wrap(output.getBytes),
        key.getOrElse(Random.nextInt.toString)
      )
    } yield p

    putData onComplete {
      case Success(result) => {
        info(s"Writing successful")
        info(s"  + ShardId: ${result.shardId}")
        info(s"  + SequenceNumber: ${result.sequenceNumber}")
      }
      case Failure(f) => {
        error(s"Writing failed.")
        error(s"  + " + f.getMessage)
      }
    }
  }
}
