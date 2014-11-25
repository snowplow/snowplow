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
package com.snowplowanalytics.snowplow.storage.kinesis.elasticsearch

// AWS Kinesis Connector libs
import com.amazonaws.services.kinesis.connectors.elasticsearch.{
  ElasticsearchObject,
  ElasticsearchEmitter,
  ElasticsearchTransformer
}
import com.amazonaws.services.kinesis.connectors.interfaces.{
  IEmitter,
  IBuffer,
  ITransformer,
  IFilter,
  IKinesisConnectorPipeline
}
import com.amazonaws.services.kinesis.connectors.KinesisConnectorConfiguration
import com.amazonaws.services.kinesis.connectors.impl.{BasicMemoryBuffer,AllPassFilter}

// TODO use a package object
import SnowplowRecord._

// This project
import sinks._

/**
* ElasticsearchPipeline class sets up the Emitter/Buffer/Transformer/Filter
*/
class ElasticsearchPipeline(streamType: String, documentIndex: String, documentType: String, goodSink: Option[ISink], badSink: ISink)
  extends IKinesisConnectorPipeline[ValidatedRecord, EmitterInput] {

  override def getEmitter(configuration: KinesisConnectorConfiguration): IEmitter[EmitterInput] = new SnowplowElasticsearchEmitter(configuration, goodSink, badSink)
  override def getBuffer(configuration: KinesisConnectorConfiguration) = new BasicMemoryBuffer[ValidatedRecord](configuration)
  override def getTransformer(c: KinesisConnectorConfiguration) = streamType match {
    case "good" => new SnowplowElasticsearchTransformer(documentIndex, documentType)
    case "bad" => new BadEventTransformer(documentIndex, documentType)
    case _ => throw new RuntimeException("\"stream-type\" must be set to \"good\" or \"bad\"")
  }
  override def getFilter(c: KinesisConnectorConfiguration) = new AllPassFilter[ValidatedRecord]()
}
