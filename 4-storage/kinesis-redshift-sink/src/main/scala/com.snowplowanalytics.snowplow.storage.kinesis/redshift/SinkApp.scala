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
package com.snowplowanalytics.snowplow.storage.kinesis.redshift

// Java
import java.io.File
import java.util.Properties

// Argot
import org.clapper.argot._

// Config
import com.typesafe.config.{Config, ConfigFactory}

// AWS libs
import com.amazonaws.auth.AWSCredentialsProvider

// AWS Kinesis Connector libs
import com.amazonaws.services.kinesis.connectors.KinesisConnectorConfiguration

// This project
import sinks._

/**
 * The entrypoint class for the Kinesis-S3 Sink applciation.
 */
object SinkApp extends App {

  // Argument specifications
  import ArgotConverters._
  Class.forName("org.postgresql.Driver").newInstance()

  // General bumf for our app
  val parser = new ArgotParser(
    programName = "generated",
    compactUsage = true,
    preUsage = Some("%s: Version %s. Copyright (c) 2013, %s.".format(
      generated.Settings.name,
      generated.Settings.version,
      generated.Settings.organization)
    )
  )

  // Optional config argument
  val config = parser.option[Config](List("config"),
                                     "filename",
                                     "Configuration file.") {
    (c, opt) =>

      val file = new File(c)
      if (file.exists) {
        ConfigFactory.parseFile(file)
      } else {
        parser.usage("Configuration file \"%s\" does not exist".format(c))
        ConfigFactory.empty()
      }
  }
  parser.parse(args)

  val conf = config.value.getOrElse(throw new RuntimeException("--config argument must be provided"))

  // TODO: make the conf file more like the Elasticsearch equivalent
  val kinesisSinkRegion = conf.getConfig("sink").getConfig("kinesis").getString("region")
  val kinesisSinkEndpoint = s"https://kinesis.${kinesisSinkRegion}.amazonaws.com"
  val kinesisSink = conf.getConfig("sink").getConfig("kinesis").getConfig("out")
  val kinesisSinkName = kinesisSink.getString("stream-name")

  val credentialConfig = conf.getConfig("sink").getConfig("aws")

  val credentials = CredentialsLookup.getCredentialsProvider(credentialConfig.getString("access-key"), credentialConfig.getString("secret-key"))

  val badSink = new KinesisSink(credentials, kinesisSinkEndpoint, kinesisSinkName)

  private val tuple: (Properties, KinesisConnectorConfiguration) = convertConfig(conf, credentials)
  val executor = new RedshiftSinkExecutor(tuple._2, badSink, tuple._1)
  executor.run()

  /**
   * This function converts the config file into the format
   * expected by the Kinesis connector interfaces.
   *
   * @param connector The configuration HOCON
   * @return A KinesisConnectorConfiguration
   */
  def convertConfig(conf: Config, credentials: AWSCredentialsProvider): (Properties, KinesisConnectorConfiguration) = {
    val props = new Properties()
    val connector = conf.resolve.getConfig("sink")

    val kinesis = connector.getConfig("kinesis")
    val kinesisIn = kinesis.getConfig("in")
    val kinesisRegion = kinesis.getString("region")
    val kEndpoint = s"https://kinesis.${kinesisSinkRegion}.amazonaws.com"
    val streamName = kinesisIn.getString("stream-name")
    val initialPosition = kinesisIn.getString("initial-position")
    val appName = kinesis.getString("app-name")

    val redshift_password = connector.getConfig("redshift").getString("password")
    val redshift_table = connector.getConfig("redshift").getString("table")
    val redshift_url = connector.getConfig("redshift").getString("url")
    val redshift_username = connector.getConfig("redshift").getString("username")
    props.setProperty("redshift_password", redshift_password)
    props.setProperty("redshift_table", redshift_table)
    props.setProperty("redshift_url", redshift_url)
    props.setProperty("redshift_username", redshift_username)

    val buffer = connector.getConfig("buffer")
    val byteLimit = buffer.getString("byte-limit")
    val recordLimit = buffer.getString("record-limit")
    val timeLimit = buffer.getString("time-limit")

    props.setProperty(KinesisConnectorConfiguration.PROP_KINESIS_INPUT_STREAM, streamName)
    props.setProperty(KinesisConnectorConfiguration.PROP_KINESIS_ENDPOINT, kEndpoint)
    props.setProperty(KinesisConnectorConfiguration.PROP_APP_NAME, appName)
    props.setProperty(KinesisConnectorConfiguration.PROP_INITIAL_POSITION_IN_STREAM, initialPosition)

    props.setProperty(KinesisConnectorConfiguration.PROP_BUFFER_BYTE_SIZE_LIMIT, byteLimit)
    props.setProperty(KinesisConnectorConfiguration.PROP_BUFFER_RECORD_COUNT_LIMIT, recordLimit)
    props.setProperty(KinesisConnectorConfiguration.PROP_BUFFER_MILLISECONDS_LIMIT, timeLimit)

    props.setProperty(KinesisConnectorConfiguration.PROP_CONNECTOR_DESTINATION, "s3")

    // So that the region of the DynamoDB table is correct
    props.setProperty(KinesisConnectorConfiguration.PROP_REGION_NAME, kinesisRegion)

    // The emit method retries sending to S3 indefinitely, so it only needs to be called once
    props.setProperty(KinesisConnectorConfiguration.PROP_RETRY_LIMIT, "1")

    (props, new KinesisConnectorConfiguration(props, credentials))
  }

}
