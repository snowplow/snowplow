 /*
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

// Java
import java.io.File
import java.util.Properties

// Config
import com.typesafe.config.{Config,ConfigFactory}

// Argot
import org.clapper.argot.ArgotParser

// AWS libs
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain

// AWS Kinesis Connector libs
import com.amazonaws.services.kinesis.connectors.KinesisConnectorConfiguration

// Scalaz
import scalaz._
import Scalaz._

// json4s
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.JsonDSL._

// Tracker
import com.snowplowanalytics.snowplow.scalatracker.Tracker
import com.snowplowanalytics.snowplow.scalatracker.SelfDescribingJson
import com.snowplowanalytics.snowplow.scalatracker.emitters.AsyncEmitter

// Common Enrich
import com.snowplowanalytics.snowplow.enrich.common.outputs.BadRow

// This project
import sinks._

// Whether the input stream contains enriched events or bad events
object StreamType extends Enumeration {
  type StreamType = Value
  val Good, Bad = Value
}


/**
 * Main entry point for the Elasticsearch sink
 */
object ElasticsearchSinkApp extends App {
  val parser = new ArgotParser(
    programName = generated.Settings.name,
    compactUsage = true,
    preUsage = Some("%s: Version %s. Copyright (c) 2013, %s.".format(
      generated.Settings.name,
      generated.Settings.version,
      generated.Settings.organization)
    )
  )

  // Optional config argument
  val config = parser.option[Config](
      List("config"), "filename", """
        |Configuration file""".stripMargin) {
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

  val configValue: Config = config.value.getOrElse(
    throw new RuntimeException("--config argument must be provided")).resolve.getConfig("sink")

  val streamType = configValue.getString("stream-type") match {
    case "good" => StreamType.Good
    case "bad" => StreamType.Bad
    case _ => throw new RuntimeException("\"stream-type\" must be set to \"good\" or \"bad\"")
  }
  val elasticsearch = configValue.getConfig("elasticsearch")
  val documentIndex = elasticsearch.getString("index")
  val documentType = elasticsearch.getString("type")

  val tracker = if (configValue.hasPath("monitoring.snowplow")) {
    SnowplowTracking.initializeTracker(configValue.getConfig("monitoring.snowplow")).some
  } else {
    None
  }

  val maxConnectionTime = configValue.getConfig("elasticsearch").getLong("max-timeout")
  val finalConfig = convertConfig(configValue)
  val goodSink = configValue.getString("sink.good") match {
    case "stdout" => Some(new StdouterrSink)
    case "elasticsearch" => None
  }
  val badSink = configValue.getString("sink.bad") match {
    case "stderr" => new StdouterrSink
    case "none" => new NullSink
    case "kinesis" => {
      val kinesis = configValue.getConfig("kinesis")
      val kinesisSink = kinesis.getConfig("out")
      val kinesisSinkName = kinesisSink.getString("stream-name")
      val kinesisSinkShards = kinesisSink.getInt("shards")
      val kinesisSinkRegion = kinesis.getString("region")
      val kinesisSinkEndpoint = s"https://kinesis.${kinesisSinkRegion}.amazonaws.com"
      new KinesisSink(finalConfig.AWS_CREDENTIALS_PROVIDER, kinesisSinkEndpoint, kinesisSinkName, kinesisSinkShards)
    }
  }

  val executor = configValue.getString("source") match {

    // Read records from Kinesis
    case "kinesis" => {
      new ElasticsearchSinkExecutor(streamType, documentIndex, documentType, finalConfig, goodSink, badSink, tracker, maxConnectionTime).success
    }

    // Run locally, reading from stdin and sending events to stdout / stderr rather than Elasticsearch / Kinesis
    // TODO reduce code duplication
    case "stdin" => new Runnable {
      val transformer = streamType match {
        case StreamType.Good => new SnowplowElasticsearchTransformer(documentIndex, documentType)
        case StreamType.Bad => new BadEventTransformer(documentIndex, documentType)
      }
      lazy val elasticsearchSender = new ElasticsearchSender(finalConfig, None, maxConnectionTime)
      def run = for (ln <- scala.io.Source.stdin.getLines) {
        val emitterInput = transformer.consumeLine(ln)
        emitterInput._2.bimap(
          f => badSink.store(FailureUtils.getBadRow(emitterInput._1, f), None, false),
          s => goodSink match {
            case Some(gs) => gs.store(s.getSource, None, true)
            case None => elasticsearchSender.sendToElasticsearch(List(ln -> s.success))
          }
        )
      }
    }.success
    case _ => "Source must be set to 'stdin' or 'kinesis'".fail
  }

  executor.fold(
    err => throw new RuntimeException(err),
    exec => {
      tracker foreach {
        t => SnowplowTracking.initializeSnowplowTracking(t)
      }
      exec.run()

      // If the stream cannot be found, the KCL's "cw-metrics-publisher" thread will prevent the
      // application from exiting naturally so we explicitly call System.exit.
      System.exit(1)
    }
  )

  /**
   * Builds a KinesisConnectorConfiguration from the "connector" field of the configuration HOCON
   *
   * @param connector The "connector" field of the configuration HOCON
   * @return A KinesisConnectorConfiguration
   */
  def convertConfig(connector: Config): KinesisConnectorConfiguration = {

    val aws = connector.getConfig("aws")
    val accessKey = aws.getString("access-key")
    val secretKey = aws.getString("secret-key")

    val elasticsearch = connector.getConfig("elasticsearch")
    val elasticsearchEndpoint = elasticsearch.getString("endpoint")
    val clusterName = elasticsearch.getString("cluster-name")

    val kinesis = connector.getConfig("kinesis")
    val kinesisIn = kinesis.getConfig("in")
    val streamRegion = kinesis.getString("region")
    val appName = kinesis.getString("app-name")
    val initialPosition = kinesisIn.getString("initial-position")
    val maxRecords = if (kinesisIn.hasPath("maxRecords")) {
      kinesisIn.getInt("maxRecords")
    } else {
      10000
    }
    val streamName = kinesisIn.getString("stream-name")
    val streamEndpoint = s"https://kinesis.${streamRegion}.amazonaws.com"

    val buffer = connector.getConfig("buffer")
    val byteLimit = buffer.getString("byte-limit")
    val recordLimit = buffer.getString("record-limit")
    val timeLimit = buffer.getString("time-limit")

    val props = new Properties

    props.setProperty(KinesisConnectorConfiguration.PROP_KINESIS_INPUT_STREAM, streamName)
    props.setProperty(KinesisConnectorConfiguration.PROP_KINESIS_ENDPOINT, streamEndpoint)
    props.setProperty(KinesisConnectorConfiguration.PROP_APP_NAME, appName)
    props.setProperty(KinesisConnectorConfiguration.PROP_INITIAL_POSITION_IN_STREAM, initialPosition)
    props.setProperty(KinesisConnectorConfiguration.PROP_MAX_RECORDS, maxRecords.toString)

    // So that the region of the DynamoDB table is correct
    props.setProperty(KinesisConnectorConfiguration.PROP_REGION_NAME, streamRegion)

    props.setProperty(KinesisConnectorConfiguration.PROP_ELASTICSEARCH_ENDPOINT, elasticsearchEndpoint)
    props.setProperty(KinesisConnectorConfiguration.PROP_ELASTICSEARCH_CLUSTER_NAME, clusterName)

    props.setProperty(KinesisConnectorConfiguration.PROP_BUFFER_BYTE_SIZE_LIMIT, byteLimit)
    props.setProperty(KinesisConnectorConfiguration.PROP_BUFFER_RECORD_COUNT_LIMIT, recordLimit)
    props.setProperty(KinesisConnectorConfiguration.PROP_BUFFER_MILLISECONDS_LIMIT, timeLimit)

    props.setProperty(KinesisConnectorConfiguration.PROP_CONNECTOR_DESTINATION, "elasticsearch")
    props.setProperty(KinesisConnectorConfiguration.PROP_RETRY_LIMIT, "1")

    new KinesisConnectorConfiguration(props, CredentialsLookup.getCredentialsProvider(accessKey, secretKey))
  }

}
