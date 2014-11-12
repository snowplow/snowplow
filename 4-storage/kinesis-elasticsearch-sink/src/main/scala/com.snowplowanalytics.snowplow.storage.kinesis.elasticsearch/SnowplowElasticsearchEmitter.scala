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

package com.snowplowanalytics.snowplow.storage.kinesis.elasticsearch

// Amazon
import com.amazonaws.services.kinesis.connectors.KinesisConnectorConfiguration
import com.amazonaws.services.kinesis.connectors.elasticsearch.{
  ElasticsearchEmitter,
  ElasticsearchObject
}
import com.amazonaws.services.kinesis.connectors.interfaces.IEmitter

// Scalaz
import scalaz._
import Scalaz._

// Scala
import scala.collection.JavaConversions._

// TODO use a package object
import SnowplowRecord._

import java.io.IOException
import java.util.ArrayList
import java.util.Collections
import java.util.List

import org.apache.commons.logging.Log
import org.apache.commons.logging.LogFactory
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequestBuilder
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus
import org.elasticsearch.action.bulk.BulkItemResponse
import org.elasticsearch.action.bulk.BulkItemResponse.Failure
import org.elasticsearch.action.bulk.BulkRequestBuilder
import org.elasticsearch.action.bulk.BulkResponse
import org.elasticsearch.action.index.IndexRequestBuilder
import org.elasticsearch.client.transport.NoNodeAvailableException
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings.ImmutableSettings
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress

import com.amazonaws.services.kinesis.connectors.KinesisConnectorConfiguration
import com.amazonaws.services.kinesis.connectors.UnmodifiableBuffer
import com.amazonaws.services.kinesis.connectors.interfaces.IEmitter

/**
 * Class to send events rejected by Elasticsearch to Kinesis
 */
class SnowplowElasticsearchEmitter(configuration: KinesisConnectorConfiguration)
  extends IEmitter[EmitterInput] {

  private val LOG = LogFactory.getLog(getClass)

  /**
   * The settings key for the cluster name.
   * 
   * Defaults to elasticsearch.
   */
  private val ELASTICSEARCH_CLUSTER_NAME_KEY = "cluster.name"

  /**
   * The settings key for transport client sniffing. If set to true, this instructs the TransportClient to
   * find all nodes in the cluster, providing robustness if the original node were to become unavailable.
   * 
   * Defaults to false.
   */
  private val ELASTICSEARCH_CLIENT_TRANSPORT_SNIFF_KEY = "client.transport.sniff"

  /**
   * The settings key for ignoring the cluster name. Set to true to ignore cluster name validation
   * of connected nodes.
   * 
   * Defaults to false.
   */
  private val ELASTICSEARCH_CLIENT_TRANSPORT_IGNORE_CLUSTER_NAME_KEY =
          "client.transport.ignore_cluster_name"

  /**
   * The settings key for ping timeout. The time to wait for a ping response from a node.
   * 
   * Default to 5s.
   */
  private val ELASTICSEARCH_CLIENT_TRANSPORT_PING_TIMEOUT_KEY = "client.transport.ping_timeout"

  /**
   * The settings key for node sampler interval. How often to sample / ping the nodes listed and connected.
   * 
   * Defaults to 5s
   */
  private val ELASTICSEARCH_CLIENT_TRANSPORT_NODES_SAMPLER_INTERVAL_KEY =
          "client.transport.nodes_sampler_interval"

  private val settings =
                ImmutableSettings.settingsBuilder()
                        .put(ELASTICSEARCH_CLUSTER_NAME_KEY, configuration.ELASTICSEARCH_CLUSTER_NAME)
                        .put(ELASTICSEARCH_CLIENT_TRANSPORT_SNIFF_KEY, configuration.ELASTICSEARCH_TRANSPORT_SNIFF)
                        .put(ELASTICSEARCH_CLIENT_TRANSPORT_IGNORE_CLUSTER_NAME_KEY,
                                configuration.ELASTICSEARCH_IGNORE_CLUSTER_NAME)
                        .put(ELASTICSEARCH_CLIENT_TRANSPORT_PING_TIMEOUT_KEY, configuration.ELASTICSEARCH_PING_TIMEOUT)
                        .put(ELASTICSEARCH_CLIENT_TRANSPORT_NODES_SAMPLER_INTERVAL_KEY,
                                configuration.ELASTICSEARCH_NODE_SAMPLER_INTERVAL)
                        .build()

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
  private val BACKOFF_PERIOD = 10000

  elasticsearchClient.addTransportAddress(new InetSocketTransportAddress(elasticsearchEndpoint, elasticsearchPort))

       
  LOG.info("ElasticsearchEmitter using elasticsearch endpoint " + elasticsearchEndpoint + ":" + elasticsearchPort)
        
        
    /**
     * Emits records to elasticsearch.
     * 1. Adds each record to a bulk index request, conditionally adding version, ttl or create if they were set in the
     * transformer.
     * 2. Executes the bulk request, returning any record specific failures to be retried by the connector library
     * pipeline, unless
     * outlined below.
     * 
     * Record specific failures (noted in the failure.getMessage() string)
     * - DocumentAlreadyExistsException means the record has create set to true, but a document already existed at the
     * specific index/type/id.
     * - VersionConflictEngineException means the record has a specific version number that did not match what existed
     * in elasticsearch.
     * To guarantee in order processing by the connector, when putting data use the same partition key for objects going
     * to the same
     * index/type/id and set sequence number for ordering.
     * - In either case, the emitter will assume that the record would fail again in the future and thus will not return
     * the record to
     * be retried.
     * 
     * Bulk request failures
     * - NoNodeAvailableException means the TransportClient could not connect to the cluster.
     * - A general Exception catches any other unexpected behavior.
     * - In either case the emitter will continue making attempts until the issue has been resolved. This is to ensure
     * that no data
     * loss occurs and simplifies restarting the application once issues have been fixed.
     */
    
    @throws[IOException]
    override def emit(buffer: UnmodifiableBuffer[EmitterInput]): List[EmitterInput] = {
        val records = buffer.getRecords()
        if (records.isEmpty()) {
            return Collections.emptyList()
        }

        val bulkRequest: BulkRequestBuilder = elasticsearchClient.prepareBulk()
        val successfulRecords = records.filter(_._2.isSuccess)
        val unsuccessfulRecords = new ArrayList[EmitterInput]
        /*for {
          recordTuple <- records
        } yield for {
          validatedRecord <- recordTuple
        } yield for {
          validRecord <- validatedRecord
        } yield {

          val indexRequestBuilder =
                  elasticsearchClient.prepareIndex(validRecord.getIndex(), validRecord.getType(), validRecord.getId())
          indexRequestBuilder.setSource(validRecord.getSource())
          val version = validRecord.getVersion()
          if (version != null) {
              indexRequestBuilder.setVersion(version)
          }
          val ttl = validRecord.getTtl()
          if (ttl != null) {
              indexRequestBuilder.setTTL(ttl)
          }
          val create = validRecord.getCreate()
          if (create != null) {
              indexRequestBuilder.setCreate(create)
          }
          bulkRequest.add(indexRequestBuilder)
        }*/

        for {
          recordTuple <- records
        } yield {
          recordTuple._2.fold(
            badRecord => {unsuccessfulRecords.add(recordTuple._1 -> badRecord.fail)},
            validRecord => {
              val indexRequestBuilder =
                      elasticsearchClient.prepareIndex(validRecord.getIndex(), validRecord.getType(), validRecord.getId())
              indexRequestBuilder.setSource(validRecord.getSource())
              val version = validRecord.getVersion()
              if (version != null) {
                  indexRequestBuilder.setVersion(version)
              }
              val ttl = validRecord.getTtl()
              if (ttl != null) {
                  indexRequestBuilder.setTTL(ttl)
              }
              val create = validRecord.getCreate()
              if (create != null) {
                  indexRequestBuilder.setCreate(create)
              }
              bulkRequest.add(indexRequestBuilder)
            }
          )
        }

        while (true) {
            try {
                val bulkResponse = bulkRequest.execute().actionGet()

                val responses = bulkResponse.getItems()
                val failures = new ArrayList[EmitterInput]()
                failures.addAll(unsuccessfulRecords)
                var numberOfSkippedRecords = 0
                for (i <- 1 until responses.length) {
                    if (responses(i).isFailed()) {
                        LOG.error("Record failed with message: " + responses(i).getFailureMessage())
                        val failure = responses(i).getFailure()
                        if (failure.getMessage().contains("DocumentAlreadyExistsException")
                                || failure.getMessage().contains("VersionConflictEngineException")) {
                            numberOfSkippedRecords += 1
                        } else {
                            failures.add(successfulRecords.get(i))
                        }
                    }
                }
                LOG.info("Emitted " + (records.size() - failures.size() - numberOfSkippedRecords)
                        + " records to Elasticsearch")
                if (!failures.isEmpty()) {
                    printClusterStatus()
                    LOG.warn("Returning " + failures.size() + " records as failed")
                }
                return failures
            } catch {
              case nnae: NoNodeAvailableException => {
                LOG.error("No nodes found at " + elasticsearchEndpoint + ":" + elasticsearchPort + ". Retrying in "
                        + BACKOFF_PERIOD + " milliseconds", nnae)
                sleep(BACKOFF_PERIOD)
              }
             case e: Exception => {
                LOG.error("ElasticsearchEmitter threw an unexpected exception ", e)
                sleep(BACKOFF_PERIOD)
              }
            }
        }

        // The compiler requires this
        throw new IllegalStateException("The while loop should only exit when the emit method returns")

    }

  override def fail(records: java.util.List[EmitterInput]) {
    // TODO: send the records to Kinesis
    println(records)
  }

  
  override def shutdown: Unit = {
      elasticsearchClient.close();
  }

  private def sleep(sleepTime: Long): Unit = {
      try {
          Thread.sleep(sleepTime);
      } catch {
        case e: InterruptedException => ()
      }
  }

  private def printClusterStatus() {
      val healthRequestBuilder = elasticsearchClient.admin().cluster().prepareHealth();
      val response = healthRequestBuilder.execute().actionGet();
      if (response.getStatus().equals(ClusterHealthStatus.RED)) {
          LOG.error("Cluster health is RED. Indexing ability will be limited");
      } else if (response.getStatus().equals(ClusterHealthStatus.YELLOW)) {
          LOG.warn("Cluster health is YELLOW.");
      } else if (response.getStatus().equals(ClusterHealthStatus.GREEN)) {
          LOG.info("Cluster health is GREEN.");
      }
  }

}
