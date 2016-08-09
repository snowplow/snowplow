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

package com.snowplowanalytics.snowplow
package storage.kinesis.elasticsearch

// Amazon
import com.amazonaws.services.kinesis.connectors.KinesisConnectorConfiguration
import com.amazonaws.services.kinesis.connectors.elasticsearch.{
  ElasticsearchEmitter,
  ElasticsearchObject
}
import com.amazonaws.services.kinesis.connectors.interfaces.IEmitter

// Elasticsearch
import org.elasticsearch.action.admin.cluster.health.{
  ClusterHealthRequestBuilder,
  ClusterHealthResponse,
  ClusterHealthStatus
}
import org.elasticsearch.action.bulk.{
  BulkItemResponse,
  BulkRequestBuilder,
  BulkResponse
}
import org.elasticsearch.action.bulk.BulkItemResponse.Failure
import org.elasticsearch.action.index.IndexRequestBuilder
import org.elasticsearch.client.transport.{
  NoNodeAvailableException,
  TransportClient
}
import org.elasticsearch.common.settings.{
  ImmutableSettings,
  Settings
}
import org.elasticsearch.common.transport.InetSocketTransportAddress

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

/**
 * Class to send valid records to Elasticsearch and invalid records to Kinesis
 *
 * @param configuration the KCL configuration
 * @param goodSink the configured GoodSink
 * @param badSink the configured BadSink
 * @param tracker a Tracker instance
 * @param maxConnectionWaitTimeMs the maximum amount of time
 *        we can attempt to send to elasticsearch
 */
class SnowplowElasticsearchEmitter(
  configuration: KinesisConnectorConfiguration,
  goodSink: Option[ISink],
  badSink: ISink,
  tracker: Option[Tracker] = None,
  maxConnectionWaitTimeMs: Long = 60000)

  extends IEmitter[EmitterInput] {

  private val Log = LogFactory.getLog(getClass)

  private val newInstance = new ElasticsearchSender(configuration, tracker, maxConnectionWaitTimeMs)

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
    newInstance.sendToElasticsearch(records)
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
