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
import com.amazonaws.AmazonServiceException
import com.amazonaws.auth.AWSCredentialsProvider

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

// Logging
import org.slf4j.LoggerFactory

// Snowplow
import com.snowplowanalytics.snowplow.collectors.thrift._
import common.outputs.CanonicalOutput

/**
 * Kinesis Sink for Scala enrichment
 */
class KinesisSink(provider: AWSCredentialsProvider,
    config: KinesisEnrichConfig) extends ISink {
  private lazy val log = LoggerFactory.getLogger(getClass())
  import log.{error, debug, info, trace}

  // Create a Kinesis client for stream interactions.
  private implicit val kinesis = Client.fromCredentials(provider)

  // The output stream for enriched events.
  private val enrichedStream = createAndLoadStream()

  /**
   * Checks if a stream exists.
   */
  def streamExists(name: String, timeout: Int = 60): Boolean = {
    val streamListFuture = for {
      s <- Kinesis.streams.list
    } yield s
    val streamList: Iterable[String] =
      Await.result(streamListFuture, Duration(timeout, SECONDS))
    for (streamStr <- streamList) {
      if (streamStr == name) {
        info(s"Stream $name exists")
        return true
      }
    }

    info(s"Stream $name doesn't exist")
    false
  }

  /**
   * Creates a new stream if one doesn't exist
   */
  def createAndLoadStream(timeout: Int = 60): Stream = {
    val name = config.enrichedOutStream
    val size = config.enrichedOutStreamShards
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

  /**
   * Side-effecting function to store the CanonicalOutput
   * to the given output stream.
   *
   * CanonicalOutput takes the form of a tab-delimited
   * String until such time as https://github.com/snowplow/snowplow/issues/211
   * is implemented.
   */
  def storeCanonicalOutput(output: String, key: String) = {
    val putData = for {
      p <- enrichedStream.put(
        ByteBuffer.wrap(output.getBytes),
        key
      )
    } yield p
    val result = Await.result(putData, Duration(60, SECONDS))
    info(s"Writing successful")
    info(s"  + ShardId: ${result.shardId}")
    info(s"  + SequenceNumber: ${result.sequenceNumber}")
  }
}
