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

package com.snowplowanalytics.snowplow.storage.kinesis.elasticsearch
package clients

// Amazon
import com.amazonaws.services.kinesis.connectors.KinesisConnectorConfiguration

// Jest
import io.searchbox.client.{
  JestClient,
  JestClientFactory
}
import io.searchbox.core._
import io.searchbox.client.config.HttpClientConfig
import io.searchbox.cluster.Health

// Java
import java.util.concurrent.TimeUnit

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
import com.snowplowanalytics.snowplow.scalatracker.Tracker

// This project
import sinks._

/**
 * Sends Elasticsearch documents via the HTTP Jest Client.
 * Batches are bulk sent to the end point.
 */
class ElasticsearchSenderHTTP(
  configuration: KinesisConnectorConfiguration,
  tracker: Option[Tracker] = None,
  maxConnectionWaitTimeMs: Long = 60000
) extends ElasticsearchSender {

  private val Log = LogFactory.getLog(getClass)

  // An ISO valid timestamp formatter
  private val TstampFormat = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").withZone(DateTimeZone.UTC)

  /**
   * Prepare the elasticsearch client
   */
  private val factory: JestClientFactory = new JestClientFactory()
  factory.setHttpClientConfig(new HttpClientConfig
    .Builder("http://" + configuration.ELASTICSEARCH_ENDPOINT + ":" + configuration.ELASTICSEARCH_PORT)
    .multiThreaded(true)
    .discoveryEnabled(false)
    .maxConnectionIdleTime(30L, TimeUnit.SECONDS)
    .connTimeout(5000)
    .readTimeout(5000)
    .build()
  )
  private val elasticsearchClient: JestClient = factory.getObject()

  /**
   * The Elasticsearch endpoint.
   */
  private val elasticsearchEndpoint = configuration.ELASTICSEARCH_ENDPOINT

  /**
   * The Elasticsearch port.
   */
  private val elasticsearchPort = configuration.ELASTICSEARCH_PORT

  /**
   * The amount of time to wait in between unsuccessful index requests (in milliseconds).
   * 10 seconds = 10 * 1000 = 10000
   */
  private val BackoffPeriod = 10000
       
  Log.info("ElasticsearchSender using elasticsearch endpoint " + elasticsearchEndpoint + ":" + elasticsearchPort)

  /**
   * Emits good records to Elasticsearch and bad records to Kinesis.
   * All valid records in the buffer get sent to Elasticsearch in a bulk request.
   * All invalid requests and all requests which failed transformation get sent to Kinesis.
   *
   * @param records List of records to send to Elasticsearch
   * @return List of inputs which Elasticsearch rejected
   */
  def sendToElasticsearch(records: List[EmitterInput]): List[EmitterInput] = {

    val actions: List[io.searchbox.core.Index] = for {
      (_, Success(record)) <- records
    } yield {
      new io.searchbox.core.Index.Builder(record.getSource)
        .index(record.getIndex)
        .`type`(record.getType)
        .id(record.getId)
        .build()
    }

    val bulkRequest = new Bulk.Builder()
      .addAction(actions)
      .build()

    val connectionAttemptStartTime = System.currentTimeMillis()

    /**
     * Keep attempting to execute the buldRequest until it succeeds
     *
     * @return List of inputs which Elasticsearch rejected
     */
    @tailrec def attemptEmit(attemptNumber: Long = 1): List[EmitterInput] = {

      if (attemptNumber > 1 && System.currentTimeMillis() - connectionAttemptStartTime > maxConnectionWaitTimeMs) {
        forceShutdown()
      }

      try {
        val bulkResponse: BulkResult = elasticsearchClient.execute(bulkRequest)
        val responses = bulkResponse.getItems

        val allFailures = responses.toList.zip(records).filter(_._1.error != null).map(pair => {
          val (response, record) = pair
          val failure = response.error

          Log.error("Record failed with message: " + failure)

          if (failure.contains("DocumentAlreadyExistsException") || failure.contains("VersionConflictEngineException")) {
            None
          } else {
            Some(record._1 -> List("Elasticsearch rejected record with message: %s".format(failure)).fail)
          }
        })

        val numberOfSkippedRecords = allFailures.count(_.isEmpty)
        val failures = allFailures.flatten

        Log.info("Emitted " + (records.size - failures.size - numberOfSkippedRecords) + " records to Elasticsearch")

        if (!failures.isEmpty) {
          printClusterStatus
          Log.warn("Returning " + failures.size + " records as failed")
        }

        failures
      } catch {
        case e: Exception => {
          Log.error("ElasticsearchEmitter threw an unexpected exception ", e)
          sleep(BackoffPeriod)
          tracker foreach {
            t => SnowplowTracking.sendFailureEvent(t, BackoffPeriod, attemptNumber, connectionAttemptStartTime, e.toString)
          }
          attemptEmit(attemptNumber + 1)
        }
      }
    }

    attemptEmit()
  }

  /**
   * Shuts the client down
   */
  def close(): Unit = {
    elasticsearchClient.shutdownClient
  }

  /**
   * Logs the Elasticsearch cluster's health
   */
  private def printClusterStatus: Unit = {
    val response = elasticsearchClient.execute(new Health.Builder().build())
    val status = response.getValue("status").toString
    if (status == "red") {
      Log.error("Cluster health is RED. Indexing ability will be limited")
    } else if (status == "yellow") {
      Log.warn("Cluster health is YELLOW.")
    } else if (status == "green") {
      Log.info("Cluster health is GREEN.")
    }
  }

  /**
   * Terminate the application in a way the KCL cannot stop
   *
   * Prevents shutdown hooks from running
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
}
