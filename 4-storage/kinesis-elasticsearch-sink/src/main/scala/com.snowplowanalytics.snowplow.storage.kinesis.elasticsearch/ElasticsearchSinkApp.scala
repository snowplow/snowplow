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

object KinesisEnrichApp extends App {
  val parser = new ArgotParser(
    programName = "atodo",//generated.Settings.name,
    compactUsage = true,
    preUsage = Some("%s: Version %s. Copyright (c) 2013, %s.".format(
      "btodo",//generated.Settings.name,
      "ctodo",//generated.Settings.version,
      "dtodo")//generated.Settings.organization)
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
  //val kinesisEnrichConfig = new KinesisEnrichConfig(
  //  config.value.getOrElse(ConfigFactory.load("default"))
  //)
/*
  val source = kinesisEnrichConfig.source match {
    case Source.Kinesis => new KinesisSource(kinesisEnrichConfig)
    case Source.Stdin => new StdinSource(kinesisEnrichConfig)
  }*/

  val configValue: Config = config.value.getOrElse(throw new RuntimeException("todo"))

  val executor = new ElasticsearchSinkExecutor(convertConfig(configValue))

  executor.run

  def convertConfig(conf: Config): KinesisConnectorConfiguration = {

    val props = new Properties

    props.setProperty(KinesisConnectorConfiguration.PROP_KINESIS_INPUT_STREAM, "enrichedfbtest")
    props.setProperty(KinesisConnectorConfiguration.PROP_KINESIS_ENDPOINT, "https://kinesis.us-east-1.amazonaws.com")
    props.setProperty(KinesisConnectorConfiguration.PROP_ELASTICSEARCH_PORT, "9300")
    props.setProperty(KinesisConnectorConfiguration.PROP_ELASTICSEARCH_ENDPOINT, "localhost")
    props.setProperty(KinesisConnectorConfiguration.DEFAULT_ELASTICSEARCH_CLUSTER_NAME, "elasticsearch")

    new KinesisConnectorConfiguration(props, new DefaultAWSCredentialsProviderChain())
  }

}

// Rigidly load the configuration file here to error when
// the enrichment process starts rather than later.
// TODO: this is duplicated from Kinesis Enrich
/*
class KinesisEnrichConfig(config: Config) {
  private val enrich = config.resolve.getConfig("enrich")

  val source = enrich.getString("source") match {
    case "kinesis" => Source.Kinesis
    case "stdin" => Source.Stdin
    case "test" => Source.Test
    case _ => throw new RuntimeException("enrich.source unknown.")
  }

  private val aws = enrich.getConfig("aws")
  val accessKey = aws.getString("access-key")
  val secretKey = aws.getString("secret-key")

  private val streams = enrich.getConfig("streams")

  private val inStreams = streams.getConfig("in")
  val rawInStream = inStreams.getString("raw")

  val appName = streams.getString("app-name")

  val initialPosition = streams.getString("initial-position")
  val streamEndpoint = streams.getString("endpoint")

  private val enrichments = enrich.getConfig("enrichments")
  private val geoIp = enrichments.getConfig("geo_ip")
  val geoIpEnabled = geoIp.getBoolean("enabled")
  val maxmindFile = {
    val path = geoIp.getString("maxmind_file")
    val file = new File(path)
    if (file.exists) {
      file
    } else {
      val uri = getClass.getResource(path).toURI
      new File(uri)
    } // TODO: add error handling if this still isn't found
  }

  private val anonIp = enrichments.getConfig("anon_ip")
  val anonIpEnabled = anonIp.getBoolean("enabled")
  val anonOctets = anonIp.getInt("anon_octets")
}
*/