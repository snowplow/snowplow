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

package com.snowplowanalytics.scalacollector

// Java
import java.nio.ByteBuffer

// Amazon
import com.amazonaws.AmazonServiceException
import com.amazonaws.auth.{
  BasicAWSCredentials,
  ClasspathPropertiesFileCredentialsProvider
}

// Scalazon (for Kinesis interaction)
import io.github.cloudify.scala.aws.kinesis.Client
import io.github.cloudify.scala.aws.kinesis.Client.ImplicitExecution._
import io.github.cloudify.scala.aws.kinesis.Definitions.{Stream,PutResult}
import io.github.cloudify.scala.aws.kinesis.KinesisDsl._

// Config
import com.typesafe.config.Config

// Concurrent libraries.
import scala.concurrent.{Future,Await,TimeoutException}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

// Thrift.
import org.apache.thrift.TSerializer
import org.apache.thrift.TDeserializer

// Logging.
import org.slf4j.LoggerFactory

import com.snowplowanalytics.generated.SnowplowEvent

import scala.collection.mutable.StringBuilder

/**
 * Interface to Kinesis for the Scala collector.
 */
object KinesisInterface {
  lazy val log = LoggerFactory.getLogger(getClass())
  import log.{error, debug, info, trace}

  // Initialize
  private implicit val kinesis = createKinesisClient(
    CollectorConfig.awsAccessKey, CollectorConfig.awsSecretKey)
  private var stream: Option[Stream] = None
  private val thriftSerializer = new TSerializer()
  private val thriftDeserializer = new TDeserializer()

  /**
   * Creates a new stream if one doesn't exist.
   * Arguments are optional - defaults to the values
   * provided in the CollectorConfig if not provided.
   *
   * @param name The name of the stream to create
   * @param size The number of shards to support for this stream
   * @param timeout How long to keep checking if the stream became active,
   * in seconds
   *
   * @return a Boolean, where:
   * 1. true means the stream was successfully created or already exists
   * 2. false means an error occurred
   */
  def createStream(
      name: String = CollectorConfig.streamName,
      size: Int = CollectorConfig.streamSize,
      timeout: Int = 60): Boolean = {
    info(s"Checking streams for $name.")
    val streamListFuture = for {
      s <- Kinesis.streams.list
    } yield s
    val streamList: Iterable[String] =
      Await.result(streamListFuture, Duration(timeout, SECONDS))
    for (streamStr <- streamList) {
      if (streamStr == name) {
        info(s"String $name already exists.")
        stream = Some(Kinesis.stream(name))
        return true
      }
    }

    info(s"Stream $name doesn't exist.")
    info(s"Creating stream $name of size $size.")
    val createStream = for {
      s <- Kinesis.streams.create(name)
    } yield s

    try {
      stream = Some(Await.result(createStream, Duration(timeout, SECONDS)))
      Await.result(stream.get.waitActive.retrying(timeout),
        Duration(timeout, SECONDS))
    } catch {
      case _: TimeoutException =>
        info("Error: Timed out.")
        false
    }
    info("Successfully created stream.")
    true
  }

  /**
   * Creates a new Kinesis client from provided AWS access key and secret
   * key. If both are set to "cpf", then authenticate using the classpath
   * properties file.
   *
   * @return the initialized AmazonKinesisClient
   */
  private def createKinesisClient(
      accessKey: String, secretKey: String): Client =
    if (isCpf(accessKey) && isCpf(secretKey)) {
      Client.fromCredentials(new ClasspathPropertiesFileCredentialsProvider())
    } else if (isCpf(accessKey) || isCpf(secretKey)) {
      throw new RuntimeException("access-key and secret-key must both be set to 'cpf', or neither of them")
    } else {
      Client.fromCredentials(accessKey, secretKey)
    }

  def storeEvent(event: SnowplowEvent, key: String): PutResult = {
    info(s"Writing Thrift record to Kinesis.")
    val result = writeRecord(
      data = ByteBuffer.wrap(thriftSerializer.serialize(event)),
      key = key
    )
    info(s"Writing successful.")
    info(s"  + ShardId: ${result.shardId}")
    info(s"  + SequenceNumber: ${result.sequenceNumber}")
    result
  }

  /**
   * Stores an event to the Kinesis stream.
   *
   * @param data The data for this record
   * @param key The partition key for this record
   * @param timeout Time in seconds to wait to put the data.
   *
   * @return A PutResult containing the ShardId and SequenceNumber
   *   of the record written to.
   */
  private def writeRecord(data: ByteBuffer, key: String,
      timeout: Int = 60): PutResult = {
    val putData = for {
      p <- stream.get.put(data, key)
    } yield p
    val putResult = Await.result(putData, Duration(timeout, SECONDS))
    putResult
  }

  def dump():String = {
    val getRecords = for {
      shards <- stream.get.shards.list
      iterators <- Future.sequence(shards.map {
        shard => implicitExecute(shard.iterator)
      })
      records <- Future.sequence(iterators.map {
        iterator => implicitExecute(iterator.nextRecords)
      })
    } yield records
    val recordChunks = Await.result(getRecords, 30.seconds)

    val sb = new StringBuilder()
    for (recordChunk <- recordChunks) {
      sb ++= "==Record chunk.\n"
      for (record <- recordChunk.records) {
        sb ++= s"sequenceNumber: ${record.sequenceNumber}\n"
        sb ++= dumpEvent(record.data.array()) + "\n"
        sb ++= s"partitionKey: ${record.partitionKey}\n"
      }
    }
    sb.toString
  }

  private def dumpEvent(data: Array[Byte]): String = {
    val event = new SnowplowEvent()
    thriftDeserializer.deserialize(event, data)
    event.toString
  }

  /**
   * Is the access/secret key set to the special value "cpf" i.e. use
   * the classpath properties file for credentials.
   *
   * @param key The key to check
   * @return true if key is cpf, false otherwise
   */
  private def isCpf(key: String): Boolean = (key == "cpf")
}
