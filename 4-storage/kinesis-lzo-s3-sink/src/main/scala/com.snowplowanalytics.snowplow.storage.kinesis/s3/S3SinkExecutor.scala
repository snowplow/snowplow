/*
 * Copyright (c) 2015 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
 */
package com.snowplowanalytics.snowplow.storage.kinesis.s3

// AWS Kinesis Connector libs
import com.amazonaws.services.kinesis.connectors.{
  KinesisConnectorConfiguration,
  KinesisConnectorExecutorBase,
  KinesisConnectorRecordProcessorFactory
}

// This project
import sinks._

/**
 * Boilerplate class for Kinessis Conenector
 */
class S3SinkExecutor(config: KinesisConnectorConfiguration, badSink: ISink) extends KinesisConnectorExecutorBase[ ValidatedRecord, EmitterInput ] {
  super.initialize(config)

  override def getKinesisConnectorRecordProcessorFactory = {
    new KinesisConnectorRecordProcessorFactory[ ValidatedRecord, EmitterInput ](new S3Pipeline(badSink), config)
  }

}
