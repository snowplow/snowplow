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
class SnowplowTranquilityEmitter(
  configuration: KinesisConnectorConfiguration,
  goodSink: Option[ISink],
  badSink: ISink,
  endpoint: String,
  tracker: Option[Tracker] = None,
  maxConnectionWaitTimeMs: Long = 60000)

  extends IEmitter[EmitterInput] {

  private val Log = LogFactory.getLog(getClass)

  // An ISO valid timestamp formatter
  private val TstampFormat = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").withZone(DateTimeZone.UTC)

  /**
   * The amount of time to wait in between unsuccessful index requests (in milliseconds).
   * 10 seconds = 10 * 1000 = 10000
   */
  private val BackoffPeriod = 10000

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
        case _ => sendToTranquility(validRecords)
      }

      invalidRecords ++ elasticsearchRejects
    }
  }

  private def sendToTranquility(records: List[EmitterInput]): List[EmitterInput] = {
    val sources: List[String] = records.map(_._2).filter(_.isSuccess).map(_.toOption.get.getSource)
    import org.apache.http.entity.StringEntity
    import org.apache.http.client.entity._
    import org.apache.http.message
    import org.apache.http.client.methods.HttpPost
    import org.apache.http.impl.client.HttpClientBuilder
    import sys.process._

    for (jsonString <- sources) {
      println("SOURCE:")
      println(jsonString)
      // val httpClient = HttpClientBuilder.create().build()
      // val request = new HttpPost(endpoint)
      // val body = new StringEntity(jsonString)
      // request.setEntity(body)
      // request.addHeader("content-type", "application/json");
      // val result = httpClient.execute(request)
      "curl -H "Content-Type: application/json" -XPOST %s".format(endpoint).!
      println("RESULT:")
      println(result)
    }

    Nil
  }

  /**
   * Handles records rejected by the SnowplowElasticsearchTransformer or by Elasticsearch
   *
   * @param records List of failed records
   */
  override def fail(records: JList[EmitterInput]) {
    records foreach {
      record => {
        val output = compact(render(
          ("line" -> record._1) ~ 
          ("errors" -> record._2.swap.getOrElse(Nil)) ~
          ("failure_tstamp" -> getTimestamp(System.currentTimeMillis()))
        ))
        badSink.store(output, None, false)
      }
    }
  }

  /**
   * Required to implement IEmitter
   */
  override def shutdown {
  }

  def formatFailure(record: EmitterInput): String = {
    compact(render(("line" -> record._1) ~ ("errors" -> record._2.swap.getOrElse(Nil))))
  }

  /**
   * Period between retrying sending events to Elasticsearch
   *
   * @param sleepTime Length of time between tries
   */
  private def sleep(sleepTime: Long): Unit = {
    try {
      Thread.sleep(sleepTime)
    } catch {
      case e: InterruptedException => ()
    }
  }

  /**
   * Terminate the application in a way the KCL cannot stop
   *
   * Prevents shutdown hooks from running
   * TODO: call this if we continually fail to POST
   */
  private def forceShutdown() {
    Log.error(s"Shutting down application as unable to connect to Elasticsearch for over $maxConnectionWaitTimeMs ms")

    tracker foreach {
      t =>
        // TODO: Instead of waiting a fixed time, use synchronous tracking or futures (when the tracker supports futures)
        SnowplowTracking.trackApplicationShutdown(t)
        sleep(5000)
    }

    Runtime.getRuntime.halt(1)
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
