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
package enrichments

import cats.Monad
import cats.data.{EitherT, NonEmptyList, ValidatedNel}
import cats.effect.Clock
import cats.implicits._
import com.snowplowanalytics.forex.CreateForex
import com.snowplowanalytics.iglu.client.Client
import com.snowplowanalytics.iglu.client.resolver.registries.RegistryLookup
import com.snowplowanalytics.iglu.core.{SchemaCriterion, SchemaKey, SelfDescribingData}
import com.snowplowanalytics.iglu.core.circe.instances._
import com.snowplowanalytics.maxmind.iplookups.CreateIpLookups
import com.snowplowanalytics.refererparser.CreateParser
import com.snowplowanalytics.weather.providers.openweather.CreateOWM
import io.circe._

import registry._
import registry.apirequest.ApiRequestEnrichment
import registry.pii.PiiPseudonymizerEnrichment
import registry.sqlquery.SqlQueryEnrichment
import utils.CirceUtils

/** Companion which holds a constructor for the EnrichmentRegistry. */
object EnrichmentRegistry {

  private val EnrichmentConfigSchemaCriterion =
    SchemaCriterion("com.snowplowanalytics.snowplow", "enrichments", "jsonschema", 1, 0)

  /**
   * Constructs our EnrichmentRegistry from the supplied JSON JValue.
   * @param json A Json representing an array of enrichment JSONs
   * @param localMode Whether to use the local MaxMind data file, enabled for tests
   * @param client The Iglu client used for schema lookup and validation
   * @return Validation boxing an EnrichmentRegistry object containing enrichments configured from
   * node
   */
  def parse[F[_]: Monad: RegistryLookup: Clock](
    json: Json,
    client: Client[F, Json],
    localMode: Boolean
  ): F[ValidatedNel[String, List[EnrichmentConf]]] =
    (for {
      sd <- EitherT.fromEither[F](
        SelfDescribingData.parse(json).leftMap(parseError => NonEmptyList.one(parseError.code))
      )
      _ <- client
        .check(sd)
        .leftMap(e => NonEmptyList.one(e.toString))
        .subflatMap { _ =>
          EnrichmentConfigSchemaCriterion.matches(sd.schema) match {
            case true => ().asRight
            case false =>
              NonEmptyList
                .one(
                  s"Schema criterion $EnrichmentConfigSchemaCriterion does not match schema ${sd.schema}"
                )
                .asLeft
          }
        }
      enrichments <- EitherT.fromEither[F](json.asArray match {
        case Some(array) => array.toList.asRight
        case _ =>
          NonEmptyList
            .one("Enrichments JSON is not an array, the schema should prevent this from happening")
            .asLeft
      })
      configs <- enrichments
        .map { json =>
          for {
            sd <- EitherT.fromEither[F](
              SelfDescribingData.parse(json).leftMap(pe => NonEmptyList.one(pe.code))
            )
            _ <- client.check(sd).leftMap(e => NonEmptyList.one(e.toString))
            conf <- EitherT.fromEither[F](
              buildEnrichmentConfig(sd.schema, sd.data, localMode).toEither
            )
          } yield conf
        }
        .sequence
        .map(_.flatten)
    } yield configs).toValidated

  // todo: ValidatedNel?
  def build[
    F[_]: Monad: CreateForex: CreateIabClient: CreateIpLookups: CreateOWM: CreateParser: CreateUaParser: sqlquery.CreateSqlQueryEnrichment
  ](
    confs: List[EnrichmentConf]
  ): EitherT[F, String, EnrichmentRegistry[F]] =
    confs.foldLeft(EitherT.pure[F, String](EnrichmentRegistry[F]())) { (er, e) =>
      e match {
        case c: ApiRequestConf => er.map(_.copy(apiRequest = c.enrichment.some))
        case c: PiiPseudonymizerConf => er.map(_.copy(piiPseudonymizer = c.enrichment.some))
        case c: SqlQueryConf =>
          for {
            enrichment <- EitherT.right(c.enrichment[F])
            registry <- er
          } yield registry.copy(sqlQuery = enrichment.some)
        case c: AnonIpConf => er.map(_.copy(anonIp = c.enrichment.some))
        case c: CampaignAttributionConf => er.map(_.copy(campaignAttribution = c.enrichment.some))
        case c: CookieExtractorConf => er.map(_.copy(cookieExtractor = c.enrichment.some))
        case c: CurrencyConversionConf =>
          for {
            enrichment <- EitherT.right(c.enrichment[F])
            registry <- er
          } yield registry.copy(currencyConversion = enrichment.some)
        case c: EventFingerprintConf => er.map(_.copy(eventFingerprint = c.enrichment.some))
        case c: HttpHeaderExtractorConf => er.map(_.copy(httpHeaderExtractor = c.enrichment.some))
        case c: IabConf =>
          for {
            enrichment <- EitherT.right(c.enrichment[F])
            registry <- er
          } yield registry.copy(iab = enrichment.some)
        case c: IpLookupsConf =>
          for {
            enrichment <- EitherT.right(c.enrichment[F])
            registry <- er
          } yield registry.copy(ipLookups = enrichment.some)
        case c: JavascriptScriptConf => er.map(_.copy(javascriptScript = c.enrichment.some))
        case c: RefererParserConf =>
          for {
            enrichment <- c.enrichment[F]
            registry <- er
          } yield registry.copy(refererParser = enrichment.some)
        case c: UaParserConf =>
          for {
            enrichment <- c.enrichment[F]
            registry <- er
          } yield registry.copy(uaParser = enrichment.some)
        case c: UserAgentUtilsConf.type => er.map(_.copy(userAgentUtils = c.enrichment.some))
        case c: WeatherConf =>
          for {
            enrichment <- c.enrichment[F]
            registry <- er
          } yield registry.copy(weather = enrichment.some)
      }
    }

  /**
   * Builds an EnrichmentConf from a Json if it has a recognized name field and matches a schema key
   * @param enrichmentConfig Json with enrichment information
   * @param schemaKey SchemaKey for the Json
   * @param localMode Whether to use local data files, enabled for tests
   * @return ValidatedNelMessage boxing Option boxing an enrichment configuration
   */
  private def buildEnrichmentConfig(
    schemaKey: SchemaKey,
    enrichmentConfig: Json,
    localMode: Boolean
  ): ValidatedNel[String, Option[EnrichmentConf]] =
    CirceUtils.extract[Boolean](enrichmentConfig, "enabled").toEither match {
      case Right(false) => None.validNel // Enrichment is disabled
      case _ =>
        (for {
          nm <- CirceUtils
            .extract[String](enrichmentConfig, "name")
            .toValidatedNel[String, String]
            .toEither
          e = if (nm == "ip_lookups") {
            IpLookupsEnrichment.parse(enrichmentConfig, schemaKey, localMode).map(_.some)
          } else if (nm == "anon_ip") {
            AnonIpEnrichment.parse(enrichmentConfig, schemaKey).map(_.some)
          } else if (nm == "referer_parser") {
            RefererParserEnrichment.parse(enrichmentConfig, schemaKey, localMode).map(_.some)
          } else if (nm == "campaign_attribution") {
            CampaignAttributionEnrichment.parse(enrichmentConfig, schemaKey).map(_.some)
          } else if (nm == "user_agent_utils_config") {
            UserAgentUtilsEnrichmentConfig.parse(enrichmentConfig, schemaKey).map(_.some)
          } else if (nm == "ua_parser_config") {
            UaParserEnrichment.parse(enrichmentConfig, schemaKey).map(_.some)
          } else if (nm == "yauaa_enrichment_config") {
            YauaaEnrichment.parse(enrichmentConfig, schemaKey).map(_.some)
          } else if (nm == "currency_conversion_config") {
            CurrencyConversionEnrichment
              .parse(enrichmentConfig, schemaKey)
              .map(_.some)
          } else if (nm == "javascript_script_config") {
            JavascriptScriptEnrichment
              .parse(enrichmentConfig, schemaKey)
              .map(_.some)
          } else if (nm == "event_fingerprint_config") {
            EventFingerprintEnrichment
              .parse(enrichmentConfig, schemaKey)
              .map(_.some)
          } else if (nm == "cookie_extractor_config") {
            CookieExtractorEnrichment
              .parse(enrichmentConfig, schemaKey)
              .map(_.some)
          } else if (nm == "http_header_extractor_config") {
            HttpHeaderExtractorEnrichment
              .parse(enrichmentConfig, schemaKey)
              .map(_.some)
          } else if (nm == "weather_enrichment_config") {
            WeatherEnrichment.parse(enrichmentConfig, schemaKey).map(_.some)
          } else if (nm == "api_request_enrichment_config") {
            ApiRequestEnrichment.parse(enrichmentConfig, schemaKey).map(_.some)
          } else if (nm == "sql_query_enrichment_config") {
            SqlQueryEnrichment.parse(enrichmentConfig, schemaKey).map(_.some)
          } else if (nm == "pii_enrichment_config") {
            PiiPseudonymizerEnrichment.parse(enrichmentConfig, schemaKey).map(_.some)
          } else if (nm == "iab_spiders_and_robots_enrichment") {
            IabEnrichment.parse(enrichmentConfig, schemaKey, localMode).map(_.some)
          } else {
            None.validNel // Enrichment is not recognized
          }
          enrichment <- e.toEither
        } yield enrichment).toValidated
    }
}

/** A registry to hold all of our enrichments. */
final case class EnrichmentRegistry[F[_]](
  apiRequest: Option[ApiRequestEnrichment] = None,
  piiPseudonymizer: Option[PiiPseudonymizerEnrichment] = None,
  sqlQuery: Option[SqlQueryEnrichment[F]] = None,
  anonIp: Option[AnonIpEnrichment] = None,
  campaignAttribution: Option[CampaignAttributionEnrichment] = None,
  cookieExtractor: Option[CookieExtractorEnrichment] = None,
  currencyConversion: Option[CurrencyConversionEnrichment[F]] = None,
  eventFingerprint: Option[EventFingerprintEnrichment] = None,
  httpHeaderExtractor: Option[HttpHeaderExtractorEnrichment] = None,
  iab: Option[IabEnrichment] = None,
  ipLookups: Option[IpLookupsEnrichment[F]] = None,
  javascriptScript: Option[JavascriptScriptEnrichment] = None,
  refererParser: Option[RefererParserEnrichment] = None,
  uaParser: Option[UaParserEnrichment] = None,
  userAgentUtils: Option[UserAgentUtilsEnrichment.type] = None,
  weather: Option[WeatherEnrichment[F]] = None,
  yauaa: Option[YauaaEnrichment] = None
)
