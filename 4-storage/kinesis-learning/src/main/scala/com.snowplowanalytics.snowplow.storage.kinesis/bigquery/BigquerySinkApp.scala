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

// Java
import java.util.Properties

// Config
import com.typesafe.config.{Config, ConfigFactory}

// AWS Kinesis Connector libs
 import com.amazonaws.services.kinesis.connectors.KinesisConnectorConfiguration

/**
 * Main entry point for the Bigquery sink.
 */
object BigquerySinkApp extends App {
  
  val con = SnowplowBigqueryEmitter.getConfigFromFile("application.conf").resolve
  val executor = new BigquerySinkExecutor(makeConf(con))
  executor.run()

  def makeConf(con: Config): KinesisConnectorConfiguration = {

    val props = new Properties

    val connector = con.getConfig("connector")

    //General properties
    val buffer = connector.getConfig("buffer")
    val byteLimit = buffer.getString("byte-limit")
    val recordLimit = buffer.getString("record-limit")
    val timeLimit = buffer.getString("time-limit")
    props.setProperty(KinesisConnectorConfiguration.PROP_BUFFER_BYTE_SIZE_LIMIT, byteLimit)
    props.setProperty(KinesisConnectorConfiguration.PROP_BUFFER_RECORD_COUNT_LIMIT, recordLimit)
    props.setProperty(KinesisConnectorConfiguration.PROP_BUFFER_MILLISECONDS_LIMIT, timeLimit)

    //Kinesis propeties
    val aws = connector.getConfig("aws")
    val accessKey = aws.getString("access-key")
    val secretKey = aws.getString("secret-key")
     
    val kinesis = connector.getConfig("kinesis")
    val kinesisIn = kinesis.getConfig("in")
    val streamRegion = kinesis.getString("region")
    val appName = kinesis.getString("app-name")
    val initialPosition = kinesisIn.getString("initial-position")
    val streamName = kinesisIn.getString("stream-name")
    val streamEndpoint = s"https://kinesis.${streamRegion}.amazonaws.com"
    props.setProperty(KinesisConnectorConfiguration.PROP_KINESIS_INPUT_STREAM, streamName)
    props.setProperty(KinesisConnectorConfiguration.PROP_KINESIS_ENDPOINT, streamEndpoint)
    props.setProperty(KinesisConnectorConfiguration.PROP_APP_NAME, appName)
    props.setProperty(KinesisConnectorConfiguration.PROP_INITIAL_POSITION_IN_STREAM, initialPosition)

    //TODO - add bigquery specific properties here

    new KinesisConnectorConfiguration(props, CredentialsLookup.getCredentialsProvider(accessKey, secretKey))

  }

}
