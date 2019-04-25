/*
 * Copyright (c) 2012-2019 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.enrich.common
package enrichments.registry

import java.net.URI

import cats.{Functor, Monad}
import cats.data.{EitherT, ValidatedNel}
import cats.syntax.either._
import com.snowplowanalytics.forex.CreateForex
import com.snowplowanalytics.forex.model.AccountType
import com.snowplowanalytics.iglu.core.{SchemaCriterion, SchemaKey}
import com.snowplowanalytics.maxmind.iplookups.CreateIpLookups
import com.snowplowanalytics.refererparser.CreateParser
import com.snowplowanalytics.weather.providers.openweather.CreateOWM
import io.circe._
import org.joda.money.CurrencyUnit
import org.mozilla.javascript.Script

import apirequest._
import sqlquery._
import utils.ConversionUtils

/** Trait inherited by every enrichment config case class */
trait Enrichment

sealed trait EnrichmentConf {
  def filesToCache: List[(URI, String)] = Nil
}
final case class ApiRequestConf(
  inputs: List[apirequest.Input],
  api: HttpApi,
  outputs: List[apirequest.Output],
  cache: apirequest.Cache
) extends EnrichmentConf {
  def enrichment[F[_]: CreateApiRequestEnrichment]: F[ApiRequestEnrichment[F]] =
    ApiRequestEnrichment[F](this)
}
final case class PiiPseudonymizerConf(
  fieldList: List[pii.PiiField],
  emitIdentificationEvent: Boolean,
  strategy: pii.PiiStrategy
) extends EnrichmentConf {
  def enrichment: pii.PiiPseudonymizerEnrichment =
    pii.PiiPseudonymizerEnrichment(fieldList, emitIdentificationEvent, strategy)
}
final case class SqlQueryConf(
  inputs: List[sqlquery.Input],
  db: Db,
  query: Query,
  output: sqlquery.Output,
  cache: sqlquery.Cache
) extends EnrichmentConf {
  def enrichment[F[_]: Monad: CreateSqlQueryEnrichment]: F[SqlQueryEnrichment[F]] =
    SqlQueryEnrichment[F](this)
}
final case class AnonIpConf(
  octets: AnonIPv4Octets.AnonIPv4Octets,
  segments: AnonIPv6Segments.AnonIPv6Segments
) extends EnrichmentConf {
  override val filesToCache: List[(URI, String)] = Nil
  def enrichment: AnonIpEnrichment = AnonIpEnrichment(octets, segments)
}
final case class CampaignAttributionConf(
  mediumParameters: List[String],
  sourceParameters: List[String],
  termParameters: List[String],
  contentParameters: List[String],
  campaignParameters: List[String],
  clickIdParameters: List[(String, String)]
) extends EnrichmentConf {
  def enrichment: CampaignAttributionEnrichment = CampaignAttributionEnrichment(
    mediumParameters,
    sourceParameters,
    termParameters,
    contentParameters,
    campaignParameters,
    clickIdParameters
  )
}
final case class CookieExtractorConf(cookieNames: List[String]) extends EnrichmentConf {
  def enrichment: CookieExtractorEnrichment = CookieExtractorEnrichment(cookieNames)
}
final case class CurrencyConversionConf(
  accountType: AccountType,
  apiKey: String,
  baseCurrency: CurrencyUnit
) extends EnrichmentConf {
  def enrichment[F[_]: Monad: CreateForex]: F[CurrencyConversionEnrichment[F]] =
    CurrencyConversionEnrichment[F](this)
}
final case class EventFingerprintConf(algorithm: String => String, excludedParameters: List[String])
    extends EnrichmentConf {
  def enrichment: EventFingerprintEnrichment =
    EventFingerprintEnrichment(algorithm, excludedParameters)
}
final case class HttpHeaderExtractorConf(headersPattern: String) extends EnrichmentConf {
  def enrichment: HttpHeaderExtractorEnrichment = HttpHeaderExtractorEnrichment(headersPattern)
}
final case class IabConf(
  ipFile: (URI, String),
  excludeUaFile: (URI, String),
  includeUaFile: (URI, String)
) extends EnrichmentConf {
  override val filesToCache: List[(URI, String)] = List(ipFile, excludeUaFile, includeUaFile)
  def enrichment[F[_]: Monad: CreateIabClient]: F[IabEnrichment] =
    IabEnrichment[F](this)
}
final case class IpLookupsConf(
  geoFile: Option[(URI, String)],
  ispFile: Option[(URI, String)],
  domainFile: Option[(URI, String)],
  connectionTypeFile: Option[(URI, String)]
) extends EnrichmentConf {
  override val filesToCache: List[(URI, String)] =
    List(geoFile, ispFile, domainFile, connectionTypeFile).flatten
  def enrichment[F[_]: Functor: CreateIpLookups]: F[IpLookupsEnrichment[F]] =
    IpLookupsEnrichment[F](this)
}
final case class JavascriptScriptConf(script: Script) extends EnrichmentConf {
  def enrichment: JavascriptScriptEnrichment = JavascriptScriptEnrichment(script)
}
final case class RefererParserConf(refererDatabase: (URI, String), internalDomains: List[String])
    extends EnrichmentConf {
  override val filesToCache: List[(URI, String)] = List(refererDatabase)
  def enrichment[F[_]: Monad: CreateParser]: EitherT[F, String, RefererParserEnrichment] =
    RefererParserEnrichment[F](this)
}
final case class UaParserConf(uaDatabase: Option[(URI, String)]) extends EnrichmentConf {
  override val filesToCache: List[(URI, String)] = List(uaDatabase).flatten
  def enrichment[F[_]: Monad: CreateUaParser]: EitherT[F, String, UaParserEnrichment] =
    UaParserEnrichment[F](this)
}
final case object UserAgentUtilsConf extends EnrichmentConf {
  def enrichment: UserAgentUtilsEnrichment.type = UserAgentUtilsEnrichment
}
final case class WeatherConf(
  apiHost: String,
  apiKey: String,
  timeout: Int,
  cacheSize: Int,
  geoPrecision: Int
) extends EnrichmentConf {
  def enrichment[F[_]: Monad: CreateOWM]: EitherT[F, String, WeatherEnrichment[F]] =
    WeatherEnrichment[F](this)
}

/** Trait to hold helpers relating to enrichment config */
trait ParseableEnrichment {

  /** The schemas supported by this enrichment */
  def supportedSchema: SchemaCriterion

  /**
   * Tentatively parses an enrichment configuration and sends back the files that need to be cached
   * prior to the EnrichmentRegistry construction.
   * @param config Json configuration for the enrichment
   * @param schemaKey Version of the schema we want to run
   * @param whether to have an enrichment conf which will produce an enrichment running locally,
   * used for testing
   * @return the configuration for this enrichment as well as the list of files it needs cached
   */
  def parse(
    config: Json,
    schemaKey: SchemaKey,
    localMode: Boolean
  ): ValidatedNel[String, EnrichmentConf]

  /**
   * Tests whether a JSON is parseable by a specific EnrichmentConfig constructor
   * @param config The JSON
   * @param schemaKey The schemaKey which needs to be checked
   * @return The JSON or an error message, boxed
   */
  def isParseable(config: Json, schemaKey: SchemaKey): Either[String, Json] =
    if (supportedSchema.matches(schemaKey)) {
      config.asRight
    } else {
      ("Schema key %s is not supported. A '%s' enrichment must have schema '%s'.")
        .format(schemaKey, supportedSchema.name, supportedSchema)
        .asLeft
    }

  /**
   * Convert the path to a file from a String to a URI.
   * @param uri URI to a database file
   * @param database Name of the database
   * @return an Either-boxed URI
   */
  protected def getDatabaseUri(uri: String, database: String): Either[String, URI] =
    ConversionUtils
      .stringToUri(uri + (if (uri.endsWith("/")) "" else "/") + database)
      .flatMap {
        case Some(u) => u.asRight
        case None => "URI to IAB file must be provided".asLeft
      }
}
