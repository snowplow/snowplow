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

package com.snowplowanalytics.collectors
package scalastream
package backends

import scalastream._
import thrift.SnowplowEvent

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
import io.github.cloudify.scala.aws.kinesis.Definitions.{
  Stream,
  PutResult,
  Record
}
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

// Mutable data structures.
import scala.collection.mutable.StringBuilder
import scala.collection.mutable.MutableList

/**
 * Kinesis Backend for the Scala collector.
 */
object KinesisBackend {
  lazy val log = LoggerFactory.getLogger(getClass())
  import log.{error, debug, info, trace}

  // Initialize
  private implicit val kinesis = createKinesisClient(
    CollectorConfig.awsAccessKey, CollectorConfig.awsSecretKey)
  private var stream: Option[Stream] = None
  private val thriftSerializer = new TSerializer()
  private val thriftDeserializer = new TDeserializer()

  // Set the current stream to $name.
  def loadStream(name: String = CollectorConfig.streamName) = {
    stream = Some(Kinesis.stream(name))
  }

  /**
   * Checks if a stream exists.
   *
   * @param name The name of the stream to check.
   * @param timeout How long to keep checking if the stream became active,
   * in seconds
   * @return true if the stream was loaded, false if the stream doesn't exist.
   */
  def streamExists(
      name: String=CollectorConfig.streamName,
      timeout: Int = 60): Boolean = {
    val streamListFuture = for {
      s <- Kinesis.streams.list
    } yield s
    val streamList: Iterable[String] =
      Await.result(streamListFuture, Duration(timeout, SECONDS))
    for (streamStr <- streamList) {
      if (streamStr == name) {
        info(s"Stream $name exists.")
        return true
      }
    }

    info(s"Stream $name doesn't exist.")
    false
  }

  /**
   * Deletes a stream.
   */
  def deleteStream(
      name: String = CollectorConfig.streamName,
      timeout: Int = 60): Unit = {
    val localStream = Kinesis.stream(name)

    info(s"Deleting stream $name.")
    val deleteStream = for {
      s <- localStream.delete
    } yield s

    Await.result(deleteStream, Duration(timeout, SECONDS))
    info("Successfully deleted stream.")
  }

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
  def createAndLoadStream(
      name: String = CollectorConfig.streamName,
      size: Int = CollectorConfig.streamSize,
      timeout: Int = 60): Boolean = {
    if (streamExists(name)) {
      loadStream(name)
      return true
    }

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
    info(s"Writing Thrift record to Kinesis: ${event.toString}")
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

  def getRecords(): List[(SnowplowEvent,Record)] = {
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

    val recordList = new MutableList[(SnowplowEvent,Record)]
    for (recordChunk <- recordChunks) {
      for (record <- recordChunk.records) {
        val event = new SnowplowEvent()
        thriftDeserializer.deserialize(event, record.data.array())
        recordList += ((event, record))
      }
    }
    recordList.toList
  }

  def getRecordsString(): String = {
    val records = getRecords()
    val sb = new StringBuilder
    for (record <- records) {
      sb ++= record._1.toString + "\n"
      sb ++= s"sequenceNumber: ${record._2.sequenceNumber}\n"
      sb ++= s"partitionKey: ${record._2.partitionKey}\n"
    }
    sb.toString
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
