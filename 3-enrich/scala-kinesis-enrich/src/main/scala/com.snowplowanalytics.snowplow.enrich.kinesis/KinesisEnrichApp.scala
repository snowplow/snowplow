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
package com.snowplowanalytics.snowplow.enrich
package kinesis

// Java
import java.io.File

// Scala
import sys.process._

// Config
import com.typesafe.config.{
  Config,
  ConfigFactory,
  ConfigRenderOptions
}

// Argot
import org.clapper.argot.ArgotParser

// Scalaz
import scalaz.{Sink => _, _}
import Scalaz._

// json4s
import org.json4s.jackson.JsonMethods._

// Iglu
import com.snowplowanalytics.iglu.client.Resolver

// Snowplow
import common.enrichments.EnrichmentRegistry
import common.utils.JsonUtils
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

  // Mandatory config argument
  val config = parser.option[Config](
      List("config"), "filename", """
        |Configuration file.""".stripMargin) {
    (c, opt) =>
      val file = new File(c)
      if (file.exists) {
        ConfigFactory.parseFile(file)
      } else {
        parser.usage("Configuration file \"%s\" does not exist".format(c))
        ConfigFactory.empty()
      }
  }

  // Optional directory of enrichment configuration JSONs
  val enrichmentsDirectory = parser.option[String](
      List("enrichments"), "filename", """
        |Directory of enrichment configuration JSONs.""".stripMargin) {
    (c, opt) =>
      val file = new File(c)
      if (file.exists) {
        c
      } else {
        parser.usage("Enrichments directory \"%s\" does not exist".format(c))
      }
  }

  parser.parse(args)

  val parsedConfig = config.value.getOrElse(throw new RuntimeException("--config argument must be provided"))

  val kinesisEnrichConfig = new KinesisEnrichConfig(parsedConfig)

  val resolverConfig = parsedConfig.resolve.getConfig("enrich").getConfig("resolver").root.render(ConfigRenderOptions.concise())

  /**
   * Build the JSON string for the enrichment configuration from
   * the JSON files in the enrichments directory
   *
   * @return enrichments JSON string
   */
  def getEnrichmentConfig: String = {
    val enrichmentJsonStrings: String = enrichmentsDirectory.value match {
      case Some(dir) => {
        val enrichmentJsonFiles = new java.io.File(dir).listFiles.filter(_.getName.endsWith(".json"))
        enrichmentJsonFiles.map(scala.io.Source.fromFile(_).mkString).mkString(",")
      }
      case None => ""
    }

    """{"schema":"iglu:com.snowplowanalytics.snowplow/enrichments/jsonschema/1-0-0","data":[%s]}""".format(enrichmentJsonStrings)
  }

  val enrichmentConfig = getEnrichmentConfig

  implicit val igluResolver: Resolver = (for {
    json <- JsonUtils.extractJson("", resolverConfig)
    resolver <- Resolver.parse(json).leftMap(_.toString)
  } yield resolver) fold (
    e => throw new RuntimeException(e),
    s => s
  )

  val registry: EnrichmentRegistry = (for {
    registryConfig <- JsonUtils.extractJson("", enrichmentConfig)
    reg <- EnrichmentRegistry.parse(fromJsonNode(registryConfig), false).leftMap(_.toString)
  } yield reg) fold (
    e => throw new RuntimeException(e),
    s => s
  )

  val filesToCache = registry.getIpLookupsEnrichment match {
    case Some(ipLookups) => ipLookups.dbsToCache
    case None => Nil
  }

  for (uriFilePair <- filesToCache) {
    val targetFile = new File(uriFilePair._2)
    if (! targetFile.exists) {
      val downloadProcessBuilder = uriFilePair._1.toURL #> targetFile // using sys.process
      downloadProcessBuilder.run
    }
  }

  val source = kinesisEnrichConfig.source match {
    case Source.Kinesis => new KinesisSource(kinesisEnrichConfig, igluResolver, registry)
    case Source.Stdin => new StdinSource(kinesisEnrichConfig, igluResolver, registry)
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

  private val streamRegion = streams.getString("region")
  val streamEndpoint = s"https://kinesis.${streamRegion}.amazonaws.com"
}
