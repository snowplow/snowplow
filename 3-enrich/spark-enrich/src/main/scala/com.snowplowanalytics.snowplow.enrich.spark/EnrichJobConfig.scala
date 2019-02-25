/*
 * Copyright (c) 2012-2019 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at
 * http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and
 * limitations there under.
 */
package com.snowplowanalytics
package snowplow.enrich
package spark

// Java
import java.net.URI

// Joda
import org.joda.time.DateTime

// Scalaz
import scalaz._
import Scalaz._

// Scopt
import scopt._

// Snowplow
import common.ValidatedNelMessage
import common.enrichments.EnrichmentRegistry
import common.loaders.Loader
import iglu.client.validation.ProcessingMessageMethods._

sealed trait EnrichJobConfig {
  def inFolder: String
  def inFormat: String
  def outFolder: String
  def badFolder: String
  def enrichments: String
  def igluConfig: String
  def local: Boolean
}

private case class RawEnrichJobConfig(
  override val inFolder: String = "",
  override val inFormat: String = "",
  override val outFolder: String = "",
  override val badFolder: String = "",
  override val enrichments: String = "",
  override val igluConfig: String = "",
  override val local: Boolean = false,
  etlTstamp: Long = 0L
) extends EnrichJobConfig

/**
 * Case class representing the configuration for the enrich job.
 * @param inFolder Folder where the input events are located
 * @param inFormat Collector format in which the data is coming in
 * @param outFolder Output folder where the enriched events will be stored
 * @param badFolder Output folder where the malformed events will be stored
 * @param enrichments JSON representing the enrichments that need performing
 * @param igluConfig JSON representing the Iglu configuration
 * @param local Whether to build a registry from local data
 * @param etlTstamp Timestamp at which the job was launched
 */
case class ParsedEnrichJobConfig(
  override val inFolder: String,
  override val inFormat: String,
  override val outFolder: String,
  override val badFolder: String,
  override val enrichments: String,
  override val igluConfig: String,
  override val local: Boolean,
  etlTstamp: DateTime,
  filesToCache: List[(URI, String)]
) extends EnrichJobConfig

object EnrichJobConfig {
  private val parser = new scopt.OptionParser[RawEnrichJobConfig]("EnrichJob") {
    head("EnrichJob")
    opt[String]("input-folder")
      .required()
      .valueName("<input folder>")
      .action((f, c) => c.copy(inFolder = f))
      .text("Folder where the input events are located")
    opt[String]("input-format")
      .required()
      .valueName("<input format>")
      .action((f, c) => c.copy(inFormat = f))
      .text("The format in which the collector is saving data")
    opt[String]("output-folder")
      .required()
      .valueName("<output folder>")
      .action((f, c) => c.copy(outFolder = f))
      .text("Output folder where the enriched events will be stored")
    opt[String]("bad-folder")
      .required()
      .valueName("<bad folder>")
      .action((f, c) => c.copy(badFolder = f))
      .text("Output folder where the malformed events will be stored")
    opt[String]("enrichments")
      .required()
      .valueName("<enrichments>")
      .action((e, c) => c.copy(enrichments = e))
      .text("Directory where the JSONs describing the enrichments are stored")
    opt[String]("iglu-config")
      .required()
      .valueName("<iglu config>")
      .action((i, c) => c.copy(igluConfig = i))
      .text("Iglu resolver configuration")
    opt[Long]("etl-timestamp")
      .required()
      .valueName("<ETL timestamp>")
      .action((t, c) => c.copy(etlTstamp = t))
      .text("Timestamp at which the job was launched, in milliseconds")
    opt[Unit]("local")
      .hidden()
      .action((_, c) => c.copy(local = true))
      .text("Whether to build a local enrichment registry")
    help("help").text("Prints this usage text")
  }

  /** Turn a RawEnrichJobConfig into a ParsedEnrichJobConfig */
  private def transform(
    c: RawEnrichJobConfig
  ): ValidatedNelMessage[ParsedEnrichJobConfig] = {
    // We try to build all the components early to detect failures before starting the job
    import singleton._
    val resolver = ResolverSingleton.getIgluResolver(c.igluConfig)
    val registry = resolver
      .flatMap(RegistrySingleton.getEnrichmentRegistry(c.enrichments, c.local)(_))
    val loader = Loader
      .getLoader(c.inFormat)
      .fold(_.toProcessingMessage.failureNel, _.successNel)
    (resolver |@| registry |@| loader) { (_, reg, _) =>
      ParsedEnrichJobConfig(
        c.inFolder,
        c.inFormat,
        c.outFolder,
        c.badFolder,
        c.enrichments,
        c.igluConfig,
        c.local,
        new DateTime(c.etlTstamp),
        filesToCache(reg))
    }
  }

  /**
   * Load a EnrichJobConfig from command line arguments.
   * @param args The command line arguments
   * @return The job config or one or more error messages boxed in a Scalaz ValidationNel
   */
  def loadConfigFrom(
    args: Array[String]
  ): ValidatedNelMessage[ParsedEnrichJobConfig] =
    parser.parse(args, RawEnrichJobConfig()).map(transform) match {
      case Some(c) => c
      case _       => "Parsing of the configuration failed".toProcessingMessage.failureNel
    }

  /**
   * Build the list of enrichment files to cache.
   * @param registry EnrichmentRegistry used to find the files that need caching
   * @return A list of URIs representing the files that need caching
   */
  private def filesToCache(registry: EnrichmentRegistry): List[(URI, String)] =
    registry.filesToCache
}
