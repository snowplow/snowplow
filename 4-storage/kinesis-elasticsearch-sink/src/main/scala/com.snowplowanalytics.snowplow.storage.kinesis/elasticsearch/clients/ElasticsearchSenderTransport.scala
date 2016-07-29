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

class ElasticsearchSenderTransport(
  configuration: KinesisConnectorConfiguration,
  tracker: Option[Tracker] = None,
  maxConnectionWaitTimeMs: Long = 60000
) extends ElasticsearchSender {

  private val Log = LogFactory.getLog(getClass)

  // An ISO valid timestamp formatter
  private val TstampFormat = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").withZone(DateTimeZone.UTC)

  /**
   * The settings key for the cluster name.
   * 
   * Defaults to elasticsearch.
   */
  private val ElasticsearchClusterNameKey = "cluster.name"

  /**
   * The settings key for transport client sniffing. If set to true, this instructs the TransportClient to
   * find all nodes in the cluster, providing robustness if the original node were to become unavailable.
   * 
   * Defaults to false.
   */
  private val ElasticsearchClientTransportSniffKey = "client.transport.sniff"

  /**
   * The settings key for ignoring the cluster name. Set to true to ignore cluster name validation
   * of connected nodes.
   * 
   * Defaults to false.
   */
  private val ElasticsearchClientTransportIgnoreClusterNameKey = "client.transport.ignore_cluster_name"

  /**
   * The settings key for ping timeout. The time to wait for a ping response from a node.
   * 
   * Default to 5s.
   */
  private val ElasticsearchClientTransportPingTimeoutKey = "client.transport.ping_timeout"

  /**
   * The settings key for node sampler interval. How often to sample / ping the nodes listed and connected.
   * 
   * Defaults to 5s
   */
  private val ElasticsearchClientTransportNodesSamplerIntervalKey = "client.transport.nodes_sampler_interval"

  private val settings = ImmutableSettings.settingsBuilder
    .put(ElasticsearchClusterNameKey,                         configuration.ELASTICSEARCH_CLUSTER_NAME)
    .put(ElasticsearchClientTransportSniffKey,                configuration.ELASTICSEARCH_TRANSPORT_SNIFF)
    .put(ElasticsearchClientTransportIgnoreClusterNameKey,    configuration.ELASTICSEARCH_IGNORE_CLUSTER_NAME)
    .put(ElasticsearchClientTransportPingTimeoutKey,          configuration.ELASTICSEARCH_PING_TIMEOUT)
    .put(ElasticsearchClientTransportNodesSamplerIntervalKey, configuration.ELASTICSEARCH_NODE_SAMPLER_INTERVAL)
    .build

  /**
   * The Elasticsearch client.
   */
  private val elasticsearchClient = new TransportClient(settings)

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

  elasticsearchClient.addTransportAddress(new InetSocketTransportAddress(elasticsearchEndpoint, elasticsearchPort))
       
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

    val bulkRequest: BulkRequestBuilder = elasticsearchClient.prepareBulk()

    records.foreach(recordTuple => recordTuple.map(record => record.map(validRecord => {
      val indexRequestBuilder = {
        val irb =
          elasticsearchClient.prepareIndex(validRecord.getIndex, validRecord.getType, validRecord.getId)
        irb.setSource(validRecord.getSource)
        val version = validRecord.getVersion
        if (version != null) {
          irb.setVersion(version)
        }
        val ttl = validRecord.getTtl
        if (ttl != null) {
          irb.setTTL(ttl)
        }
        val create = validRecord.getCreate
        if (create != null) {
          irb.setCreate(create)
        }
        irb
      }
      bulkRequest.add(indexRequestBuilder)
    })))

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
        val bulkResponse = bulkRequest.execute.actionGet
        val responses = bulkResponse.getItems

        val allFailures = responses.toList.zip(records).filter(_._1.isFailed).map(pair => {
          val (response, record) = pair
          val failure = response.getFailure

          Log.error("Record failed with message: " + response.getFailureMessage)
          
          if (failure.getMessage.contains("DocumentAlreadyExistsException") || failure.getMessage.contains("VersionConflictEngineException")) {
            None
          } else {
            Some(record._1 -> List("Elasticsearch rejected record with message: %s".format(failure.getMessage)).fail)
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
        case nnae: NoNodeAvailableException => {
          Log.error("No nodes found at " + elasticsearchEndpoint + ":" + elasticsearchPort + ". Retrying in "
            + BackoffPeriod + " milliseconds", nnae)
          sleep(BackoffPeriod)
          tracker foreach {
            t => SnowplowTracking.sendFailureEvent(t, BackoffPeriod, attemptNumber, connectionAttemptStartTime, nnae.toString)
          }
          attemptEmit(attemptNumber + 1)
        }
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
    elasticsearchClient.close
  }

  /**
   * Logs the Elasticsearch cluster's health
   */
  private def printClusterStatus: Unit = {
    val healthRequestBuilder = elasticsearchClient.admin.cluster.prepareHealth()
    val response = healthRequestBuilder.execute.actionGet
    if (response.getStatus.equals(ClusterHealthStatus.RED)) {
      Log.error("Cluster health is RED. Indexing ability will be limited")
    } else if (response.getStatus.equals(ClusterHealthStatus.YELLOW)) {
      Log.warn("Cluster health is YELLOW.")
    } else if (response.getStatus.equals(ClusterHealthStatus.GREEN)) {
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
