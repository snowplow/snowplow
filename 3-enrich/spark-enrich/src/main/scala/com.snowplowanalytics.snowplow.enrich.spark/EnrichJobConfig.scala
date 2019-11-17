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
import cats.Id
import cats.data.{EitherNel, NonEmptyList, ValidatedNel}
import cats.syntax.either._
import cats.syntax.show._
import cats.syntax.validated._
import cats.syntax.apply._

// circe
import io.circe.Json

// Joda
import org.joda.time.DateTime

// Decline
import com.monovore.decline.{Argument, Command, Opts}

import com.snowplowanalytics.iglu.client.Client

// Snowplow
import com.snowplowanalytics.snowplow.enrich.common.enrichments.EnrichmentRegistry
import com.snowplowanalytics.snowplow.enrich.common.loaders.Loader
import com.snowplowanalytics.snowplow.enrich.common.utils.{ConversionUtils, JsonUtils}

final case class EnrichJobConfig(
  inFolder: String,
  inFormat: String,
  outFolder: String,
  badFolder: String,
  enrichments: EnrichJobConfig.Base64Json,
  igluConfig: EnrichJobConfig.Base64Json,
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
  case class ParsedEnrichJobConfig(
    inFolder: String,
    inFormat: String,
    outFolder: String,
    badFolder: String,
    enrichments: Json,
    igluConfig: Json,
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
  val enrichments = Opts.option[Base64Json](
    "enrichments",
    "Base64-encoded JSON string with enrichment configurations",
    "e",
    "base64")
  val igluConfig =
    Opts.option[Base64Json]("iglu-config", "Iglu resolver configuration JSON", "r", "base64")
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
    Command("Snowplow Spark Enrich", s"${generated.BuildInfo.name}-${generated.BuildInfo.version}")(
      enrichedJobConfig)

  /** Turn a RawEnrichJobConfig into a ParsedEnrichJobConfig */
  private def transform(
    c: EnrichJobConfig
  ): EitherNel[String, ParsedEnrichJobConfig] = {
    val q = for {
      client <- Client.parseDefault[Id](c.igluConfig.value).leftMap(e => NonEmptyList.one(e.show))
      configs <- EnrichmentRegistry
        .parse[Id](c.enrichments.value, client, c.local)
        .toEither
        .toEitherT[Id]
      filesToCache = if (c.local) Nil else configs.flatMap(_.filesToCache)
      _ <- EnrichmentRegistry.build(configs).leftMap(e => NonEmptyList.one(e))
      _ <- Loader.getLoader(c.inFormat).toEitherNel.toEitherT[Id]
    } yield
      ParsedEnrichJobConfig(
        c.inFolder,
        c.inFormat,
        c.outFolder,
        c.badFolder,
        c.enrichments.value,
        c.igluConfig.value,
        c.local,
        new DateTime(c.etlTstamp),
        filesToCache)
    q.value
  }

  /**
   * Load a EnrichJobConfig from command line arguments.
   * @param args The command line arguments
   * @return The job config or one or more error messages boxed in a Scalaz ValidationNel
   */
  def loadConfigFrom(args: Array[String]): ValidatedNel[String, ParsedEnrichJobConfig] = {
    command
      .parse(args)
      .leftMap(e => NonEmptyList.one(e.toString()))
      .flatMap(transform) match {
      case Right(c)    => c.validNel
      case Left(error) => error.toString.invalidNel
    }
  }

  case class Base64Json(value: Json) extends AnyVal

  implicit def base64Json: Argument[Base64Json] =
    new Argument[Base64Json] {
      def read(string: String): ValidatedNel[String, Base64Json] = {
        val result = for {
          raw  <- ConversionUtils.decodeBase64Url(string)
          json <- JsonUtils.extractJson(raw)
        } yield Base64Json(json)
        result.toValidatedNel
      }

      def defaultMetavar: String = "base64"
    }
}
