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

// AWS Kinesis Connector libs
import com.amazonaws.services.kinesis.connectors.elasticsearch.ElasticsearchObject
import com.amazonaws.services.kinesis.connectors.{
  KinesisConnectorConfiguration,
  KinesisConnectorExecutorBase,
  KinesisConnectorRecordProcessorFactory
}

/**
* Boilerplate class for Kinesis Conenector
*/
class BigquerySinkExecutor(config: KinesisConnectorConfiguration)
  extends KinesisConnectorExecutorBase[IntermediateRecord, IntermediateRecord]{

  val bigqueryConfig = SnowplowBigqueryEmitter.getConfigFromFile("application.conf")
  val projectNumber = bigqueryConfig.getString("connector.bigquery.project-number")
  val datasetName = bigqueryConfig.getString("connector.bigquery.dataset-name")
  val tableName = bigqueryConfig.getString("connector.bigquery.table-name")

  initialize(config)
  override def getKinesisConnectorRecordProcessorFactory = {
    new KinesisConnectorRecordProcessorFactory[IntermediateRecord, IntermediateRecord](
      new BigqueryPipeline(projectNumber, datasetName, tableName), config)
  }
}
