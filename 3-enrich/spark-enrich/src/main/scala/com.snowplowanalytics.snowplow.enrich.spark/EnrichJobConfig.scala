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

// Decline
import com.monovore.decline.Visibility

// cats
import cats.implicits._

// Joda
import org.joda.time.DateTime

// Scalaz
import scalaz._
import Scalaz._

// Scopt
import com.monovore.decline.{Command, Opts}

// Snowplow
import common.ValidatedNelMessage
import common.enrichments.EnrichmentRegistry
import common.loaders.Loader
import iglu.client.validation.ProcessingMessageMethods._

final case class EnrichJobConfig(
  inFolder: String,
  inFormat: String,
  outFolder: String,
  badFolder: String,
  enrichments: String,
  igluConfig: String,
  local: Boolean,
  etlTstamp: Long)

object EnrichJobConfig {

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
  case class ParsedEnrichJobConfig(inFolder: String,
                                   inFormat: String,
                                   outFolder: String,
                                   badFolder: String,
                                   enrichments: String,
                                   igluConfig: String,
                                   local: Boolean,
                                   etlTstamp: DateTime,
                                   filesToCache: List[(URI, String)])

  val inFolder =
    Opts.option[String]("input-folder", "Folder where the input events are located", "i", "folder")
  val inFormat = Opts.option[String](
    "input-format",
    "The format in which the collector is saving data",
    "f",
    "format")
  val outFolder = Opts.option[String](
    "output-folder",
    "Output folder where the enriched events will be stored",
    "o",
    "folder")
  val badFolder = Opts.option[String](
    "bad-folder",
    "Output folder where the malformed events will be stored",
    "b",
    "folder")
  val enrichments = Opts.option[String](
    "enrichments",
    "Base64-encoded JSON string with enrichment configurations",
    "e",
    "base64")
  val igluConfig =
    Opts.option[String]("iglu-config", "Iglu resolver configuration JSON", "r", "base64")
  val etlTstamp =
    Opts.option[Long]("etl-timestamp", "Iglu resolver configuration JSON", "r", "base64")
  val local =
    Opts
      .flag("local", "Whether to build a local enrichment registry", "l", Visibility.Partial)
      .orFalse

  val enrichedJobConfig: Opts[EnrichJobConfig] =
    (inFolder, inFormat, outFolder, badFolder, enrichments, igluConfig, local, etlTstamp).mapN(
      EnrichJobConfig.apply)

  val command =
    Command("Snowplow Spark Enrich", s"spark-enrich-${BuildInfo.version}")(enrichedJobConfig)

  /** Turn a RawEnrichJobConfig into a ParsedEnrichJobConfig */
  private def transform(
    c: EnrichJobConfig
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
  def loadConfigFrom(args: Array[String]): ValidatedNelMessage[ParsedEnrichJobConfig] =
    command.parse(args).map(transform) match {
      case Right(c)    => c
      case Left(error) => error.toString.toProcessingMessage.failureNel
    }

  /**
   * Build the list of enrichment files to cache.
   * @param registry EnrichmentRegistry used to find the files that need caching
   * @return A list of URIs representing the files that need caching
   */
  private def filesToCache(registry: EnrichmentRegistry): List[(URI, String)] =
    registry.filesToCache
}
