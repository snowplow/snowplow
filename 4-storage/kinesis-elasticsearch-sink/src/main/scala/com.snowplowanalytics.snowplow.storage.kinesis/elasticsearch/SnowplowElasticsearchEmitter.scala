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
import com.amazonaws.services.kinesis.connectors.KinesisConnectorConfiguration
import com.amazonaws.services.kinesis.connectors.elasticsearch.{
  ElasticsearchEmitter,
  ElasticsearchObject
}
import com.amazonaws.services.kinesis.connectors.interfaces.IEmitter
import com.amazonaws.services.kinesis.connectors.{
  KinesisConnectorConfiguration,
  UnmodifiableBuffer
}

// Java
import java.io.IOException
import java.util.{List => JList}

// Joda-Time
import org.joda.time.{DateTime, DateTimeZone}
import org.joda.time.format.DateTimeFormat

// Scala
import scala.collection.JavaConversions._
import scala.annotation.tailrec

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
 * @param elasticsearchClientType The type of ES Client to use
 */
class SnowplowElasticsearchEmitter(
  configuration: KinesisConnectorConfiguration,
  goodSink: Option[ISink],
  badSink: ISink,
  tracker: Option[Tracker] = None,
  maxConnectionWaitTimeMs: Long = 60000,
  elasticsearchClientType: String = "transport",
  connTimeout: Int = 300000,
  readTimeout: Int = 300000
) extends IEmitter[EmitterInput] {

  private val Log = LogFactory.getLog(getClass)

  private val newInstance: ElasticsearchSender = (
    if (elasticsearchClientType == "http") {
      new ElasticsearchSenderHTTP(configuration, tracker, maxConnectionWaitTimeMs, connTimeout, readTimeout)
    } else {
      new ElasticsearchSenderTransport(configuration, tracker, maxConnectionWaitTimeMs)
    }
  )

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

    /**
     * Add to the accumulator while conditions are met,
     * Once conditions exceeded close buffer and increment the index.
     *
     * @param records The window of records to be split
     * @param index The index of the buffer to add to
     * @param accumulator The recursive accumulator of buffers
     * @return the List of buffers to send
     */
    @scala.annotation.tailrec
    def splitBufferRec(
      records: List[EmitterInput],
      totalByteCount: Long = 0,
      index: Int = 0,
      accumulator: List[List[EmitterInput]] = Nil): List[List[EmitterInput]] = {

      if (records.length <= 0) {
        accumulator
      } else {
        val currentBuffer: List[EmitterInput] = accumulator match {
          case Nil   => List()
          case accum => accum(index)
        }

        val record: EmitterInput = records(0)
        val byteCount: Long = record match {
          case (_, Success(obj)) => obj.toString.getBytes("UTF-8").length
          case (_, Failure(_))   => 0 // This record will be ignored in the sender
        }
        val updatedRecords: List[EmitterInput] = records.drop(1)

        if ((currentBuffer.length + 1) <= recordLimit && (totalByteCount + byteCount) <= byteLimit) {
          val finalBuffer: List[EmitterInput] = currentBuffer ++ List(record)
          val finalAccumulator = (
            if (accumulator == Nil) {
              List(finalBuffer)
            } else {
              accumulator.patch(index, List(finalBuffer), 1)
            }
          )
          splitBufferRec(updatedRecords, (totalByteCount + byteCount), index, finalAccumulator)
        } else {
          splitBufferRec(updatedRecords, byteCount, index + 1, accumulator ++ List(List(record)))
        }
      }
    }

    splitBufferRec(records)
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
