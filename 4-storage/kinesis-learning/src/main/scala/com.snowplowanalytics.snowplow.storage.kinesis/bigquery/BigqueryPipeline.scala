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

package com.snowplowanalytics.snowplow.storage.kinesis.bigquery

/**
* ElasticsearchPipeline class sets up the Emitter/Buffer/Transformer/Filter
*/
class BigqueryPipeline(
  //streamType: StreamType, 
  projectNumber: String,
  datasetName: String, 
  tableName: String
) extends IKinesisConnectorPipeline[ValidatedRecord, EmitterInput] {

  override def getEmitter(configuration: KinesisConnectorConfiguration): IEmitter[EmitterInput] 
    = new SnowplowBigqueryEmitter(configuration)

  override def getBuffer(configuration: KinesisConnectorConfiguration) 
    = new BasicMemoryBuffer[ValidatedRecord](configuration)

  override def getTransformer(c: KinesisConnectorConfiguration) 
    = new SnowplowBigqueryTransformer(datasetName, tableName)

  override def getFilter(c: KinesisConnectorConfiguration)
    = new AllPassFilter[ValidateRecord]()
}
