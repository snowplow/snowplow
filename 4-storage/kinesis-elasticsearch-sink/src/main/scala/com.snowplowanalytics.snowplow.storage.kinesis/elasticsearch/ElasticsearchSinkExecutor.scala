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

// AWS Kinesis Connector libs
import com.amazonaws.services.kinesis.connectors.elasticsearch.ElasticsearchObject
import com.amazonaws.services.kinesis.connectors.{
  KinesisConnectorConfiguration,
  KinesisConnectorExecutorBase,
  KinesisConnectorRecordProcessorFactory
}

// Tracker
import scalatracker.Tracker

// This project
import sinks._
import StreamType._

/**
 * Boilerplate class for Kinesis Conenector
 *
 * @param streamType the type of stream, good/bad
 * @param documentIndex the elasticsearch index name
 * @param documentType the elasticsearch index type
 * @param config the KCL configuration
 * @param goodSink the configured GoodSink
 * @param badSink the configured BadSink
 * @param tracker a Tracker instance
 * @param maxConnectionTimeout the maximum amount of time
 *        we can attempt to send to elasticsearch
 * @param elasticsearchClientType The type of ES Client to use
 */
class ElasticsearchSinkExecutor(
  streamType: StreamType,
  documentIndex: String,
  documentType: String,
  config: KinesisConnectorConfiguration,
  goodSink: Option[ISink],
  badSink: ISink,
  tracker: Option[Tracker] = None,
  maxConnectionTimeout: Long = 60000,
  elasticsearchClientType: String
) extends KinesisConnectorExecutorBase[ValidatedRecord, EmitterInput] {

  initialize(config)
  override def getKinesisConnectorRecordProcessorFactory = {
    new KinesisConnectorRecordProcessorFactory[ValidatedRecord, EmitterInput](
      new ElasticsearchPipeline(streamType, documentIndex, documentType, goodSink, badSink, tracker, maxConnectionTimeout, elasticsearchClientType), config)
  }
}
