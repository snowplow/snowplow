/**
 * Copyright (c) 2014-2016 Snowplow Analytics Ltd.
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
package storage.kinesis.elasticsearch

// Amazon
import com.amazonaws.services.kinesis.connectors.interfaces.IEmitter
import com.amazonaws.services.kinesis.connectors.{
  KinesisConnectorConfiguration,
  UnmodifiableBuffer
}

import scala.collection.mutable.ListBuffer

// Java
import java.io.IOException
import java.util.{List => JList}

// Joda-Time
import org.joda.time.{DateTime, DateTimeZone}
import org.joda.time.format.DateTimeFormat

// Scala
import scala.collection.JavaConversions._

// Scalaz
import scalaz._
import Scalaz._

// json4s
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.JsonDSL._

// Logging
import org.apache.commons.logging.{
  Log,
  LogFactory
}

// Tracker
import scalatracker.Tracker
import scalatracker.SelfDescribingJson

// Common Enrich
import com.snowplowanalytics.snowplow.enrich.common.outputs.BadRow

// This project
import sinks._
import clients._
import generated._

/**
 * Class to send valid records to Elasticsearch and invalid records to Kinesis
 *
 * @param configuration the KCL configuration
 * @param goodSink the configured GoodSink
 * @param badSink the configured BadSink
 * @param tracker a Tracker instance
 * @param maxConnectionWaitTimeMs the maximum amount of time
 *        we can attempt to send to elasticsearch
 * @param elasticsearchSender an ElasticsearchSender instance to use
 */
class SnowplowElasticsearchEmitter(
  configuration: KinesisConnectorConfiguration,
  goodSink: Option[ISink],
  badSink: ISink,
  tracker: Option[Tracker] = None,
  maxConnectionWaitTimeMs: Long = 60000,
  elasticsearchSender: Option[ElasticsearchSender] = None,
  connTimeout: Int = 300000,
  readTimeout: Int = 300000
) extends IEmitter[EmitterInput] {

  private val Log = LogFactory.getLog(getClass)

  private val newInstance: ElasticsearchSender = elasticsearchSender match {
    case None => new ElasticsearchSenderTransport(configuration, tracker, maxConnectionWaitTimeMs)
    case Some(s) => s
  }

  // An ISO valid timestamp formatter
  private val TstampFormat = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").withZone(DateTimeZone.UTC)

  /**
   * Emits good records to stdout or Elasticsearch.
   * All records which Elasticsearch rejects and all records which failed transformation
   * get sent to to stderr or Kinesis.
   *
   * @param buffer BasicMemoryBuffer containing EmitterInputs
   * @return list of inputs which failed transformation or which Elasticsearch rejected
   */
  @throws[IOException]
  override def emit(buffer: UnmodifiableBuffer[EmitterInput]): JList[EmitterInput] = {

    val records = buffer.getRecords.toList

    if (records.isEmpty) {
      Nil
    } else {

      val (validRecords, invalidRecords) = records.partition(_._2.isSuccess)

      // Send all valid records to stdout / Elasticsearch and return those rejected by Elasticsearch
      val elasticsearchRejects = goodSink match {
        case Some(s) => {
          validRecords.foreach(recordTuple =>
            recordTuple.map(record => record.map(r => s.store(r.getSource, None, true))))
          Nil
        }
        case None if validRecords.isEmpty => Nil
        case _ => sendToElasticsearch(validRecords)
      }

      invalidRecords ++ elasticsearchRejects
    }
  }

  /**
   * Emits good records to Elasticsearch and bad records to Kinesis.
   * All valid records in the buffer get sent to Elasticsearch in a bulk request.
   * All invalid requests and all requests which failed transformation get sent to Kinesis.
   *
   * @param records List of records to send to Elasticsearch
   * @return List of inputs which Elasticsearch rejected
   */
  private def sendToElasticsearch(records: List[EmitterInput]): List[EmitterInput] = {
    val failures = for {
      recordSlice <- splitBuffer(configuration, records)
    } yield {
      newInstance.sendToElasticsearch(recordSlice)
    }
    failures.flatten
  }

  /**
   * Splits the buffer into emittable chunks based on the 
   * buffer settings defined in the config
   *
   * @param records The records to split
   * @returns a list of buffers
   */
  protected def splitBuffer(
    configuration: KinesisConnectorConfiguration, 
    records: List[EmitterInput]): List[List[EmitterInput]] = {

    val byteLimit: Long   = configuration.BUFFER_BYTE_SIZE_LIMIT
    val recordLimit: Long = configuration.BUFFER_RECORD_COUNT_LIMIT

    // partition the records in
    val remaining: ListBuffer[EmitterInput] = records.to[ListBuffer]
    val buffers: ListBuffer[List[EmitterInput]] = new ListBuffer
    val curBuffer: ListBuffer[EmitterInput] = new ListBuffer
    var runningByteCount: Long = 0

    while (remaining.nonEmpty) {
      val record = remaining.remove(0)

      val byteCount: Long = record match {
        case (_, Success(obj)) => obj.toString.getBytes("UTF-8").length
        case (_, Failure(_))   => 0 // This record will be ignored in the sender
      }

      if ((curBuffer.length + 1) > recordLimit || (runningByteCount + byteCount) > byteLimit) {
        // add this buffer to the output and start a new one with this record
        // (if the first record is larger than the byte limit the buffer will be empty)
        if (curBuffer.nonEmpty) {
          buffers += curBuffer.toList
          curBuffer.clear()
        }
        curBuffer += record
        runningByteCount = byteCount
      }

      else {
        curBuffer += record
        runningByteCount += byteCount
      }
    }

    // add any remaining items to the final buffer
    if (curBuffer.nonEmpty) {
      buffers += curBuffer.toList
    }

    buffers.toList
  }

  /**
   * Handles records rejected by the SnowplowElasticsearchTransformer or by Elasticsearch
   *
   * @param records List of failed records
   */
  override def fail(records: JList[EmitterInput]) {
    records foreach {
      record => {
        val output = FailureUtils.getBadRow(record._1, record._2.swap.getOrElse(Nil))
        badSink.store(output, None, false)
      }
    }
  }

  /**
   * Closes the Elasticsearch client when the KinesisConnectorRecordProcessor is shut down
   */
  override def shutdown {
    newInstance.close
  }

  /**
   * Returns an ISO valid timestamp
   *
   * @param tstamp The Timestamp to convert
   * @return the formatted Timestamp
   */
  private def getTimestamp(tstamp: Long): String = {
    val dt = new DateTime(tstamp)
    TstampFormat.print(dt)
  }
}
