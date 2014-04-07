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
package com.snowplowanalytics.snowplow.enrich.kinesis

// Java
import java.io.File

// Config
import com.typesafe.config.{Config,ConfigFactory}

// Argot
import org.clapper.argot.ArgotParser

// Snowplow
import sources._
import sinks._

// The enrichment process takes input SnowplowRawEvent objects from
// an input source out outputs enriched objects to a sink,
// as defined in the following enumerations.
object Source extends Enumeration {
  type Source = Value
  val Kinesis, Stdin, Test = Value
}
object Sink extends Enumeration {
  type Sink = Value
  val Kinesis, Stdouterr, Test = Value
}

// The main entry point of the Scala Kinesis Enricher.
object KinesisEnrichApp extends App {
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
        |Configuration file. Defaults to \"resources/default.conf\"
        |(within .jar) if not set""".stripMargin) {
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
  val kinesisEnrichConfig = new KinesisEnrichConfig(
    config.value.getOrElse(ConfigFactory.load("default"))
  )

  val source = kinesisEnrichConfig.source match {
    case Source.Kinesis => new KinesisSource(kinesisEnrichConfig)
    case Source.Stdin => new StdinSource(kinesisEnrichConfig)
  }
  source.run
}

// Rigidly load the configuration file here to error when
// the enrichment process starts rather than later.
class KinesisEnrichConfig(config: Config) {
  private val enrich = config.resolve.getConfig("enrich")

  val source = enrich.getString("source") match {
    case "kinesis" => Source.Kinesis
    case "stdin" => Source.Stdin
    case "test" => Source.Test
    case _ => throw new RuntimeException("enrich.source unknown.")
  }

  val sink = enrich.getString("sink") match {
    case "kinesis" => Sink.Kinesis
    case "stdouterr" => Sink.Stdouterr
    case "test" => Sink.Test
    case _ => throw new RuntimeException("enrich.sink unknown.")
  }

  private val aws = enrich.getConfig("aws")
  val accessKey = aws.getString("access-key")
  val secretKey = aws.getString("secret-key")

  private val streams = enrich.getConfig("streams")

  private val inStreams = streams.getConfig("in")
  val rawInStream = inStreams.getString("raw")

  private val outStreams = streams.getConfig("out")
  val enrichedOutStream = outStreams.getString("enriched")
  val enrichedOutStreamShards = outStreams.getInt("enriched_shards")
  val badOutStream = outStreams.getString("bad")
  val badOutStreamShards = outStreams.getInt("bad_shards")

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
