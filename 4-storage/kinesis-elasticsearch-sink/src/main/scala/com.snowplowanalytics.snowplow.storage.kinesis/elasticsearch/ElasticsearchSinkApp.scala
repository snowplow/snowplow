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
    throw new RuntimeException("--config argument must be provided")).resolve.getConfig("connector")

  val streamType = configValue.getConfig("kinesis").getConfig("in").getString("stream-type") match {
    case "good" => StreamType.Good
    case "bad" => StreamType.Bad
    case _ => throw new RuntimeException("\"stream-type\" must be set to \"good\" or \"bad\"")
  }
  val location = configValue.getConfig("location")
  val documentIndex = location.getString("index")
  val documentType = location.getString("type")

  val executor = configValue.getString("source") match {

    // Read records from Kinesis
    case "kinesis" => {
      val (goodSink, badSink) = configValue.getString("sink") match {
        case "elasticsearch-kinesis" => {
          val kinesis = configValue.getConfig("kinesis")
          val kinesisSink = kinesis.getConfig("out")
          val kinesisSinkName = kinesisSink.getString("stream-name")
          val kinesisSinkShards = kinesisSink.getInt("shards")
          val kinesisSinkRegion = kinesis.getString("region")
          val kinesisSinkEndpoint = s"https://kinesis.${kinesisSinkRegion}.amazonaws.com"
          (None, new KinesisSink(new DefaultAWSCredentialsProviderChain,
            kinesisSinkEndpoint, kinesisSinkName, kinesisSinkShards))
        }
        case "stdouterr" => (Some(new StdouterrSink), new StdouterrSink)
        case _ => throw new RuntimeException("Sink type must be 'stdouterr' or 'kinesis'")
      }

      new ElasticsearchSinkExecutor(streamType, documentIndex, documentType, convertConfig(configValue), goodSink, badSink).success
    }

    // Run locally, reading from stdin and sending events to stdout / stderr rather than Elasticsearch / Kinesis
    // TODO reduce code duplication
    case "stdin" => new Runnable {
      val transformer = new SnowplowElasticsearchTransformer(documentIndex, documentType)
      def run = for (ln <- scala.io.Source.stdin.getLines) {
        val emitterInput = transformer.fromClass(ln -> transformer.jsonifyGoodEvent(ln.split("\t", -1))
          .leftMap(_.list))
        emitterInput._2.bimap(
          f => Console.err.println(compact(render(("line" -> emitterInput._1) ~ ("errors" -> f)))),
          s => println(s.getSource)
        )
      }
    }.success
    case _ => "Source must be set to 'stdin' or 'kinesis'".fail
  }

  executor.fold(
    err => throw new RuntimeException(err),
    exec => exec.run()
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
