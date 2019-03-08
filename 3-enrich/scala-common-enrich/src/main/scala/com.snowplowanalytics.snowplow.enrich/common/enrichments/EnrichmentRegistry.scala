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

import java.net.URI

import com.snowplowanalytics.iglu.client.{Resolver, SchemaCriterion, SchemaKey}
import com.snowplowanalytics.iglu.client.validation.ValidatableJsonMethods._
import com.snowplowanalytics.iglu.client.validation.ProcessingMessageMethods._
import scalaz._
import Scalaz._
import org.json4s._
import org.json4s.jackson.JsonMethods._

import registry._
import registry.apirequest.{ApiRequestEnrichment, ApiRequestEnrichmentConfig}
import registry.pii.PiiPseudonymizerEnrichment
import registry.sqlquery.{SqlQueryEnrichment, SqlQueryEnrichmentConfig}
import utils.ScalazJson4sUtils

/**
 * Companion which holds a constructor
 * for the EnrichmentRegistry.
 */
object EnrichmentRegistry {

  implicit val formats = DefaultFormats

  private val EnrichmentConfigSchemaCriterion =
    SchemaCriterion("com.snowplowanalytics.snowplow", "enrichments", "jsonschema", 1, 0)

  /**
   * Constructs our EnrichmentRegistry
   * from the supplied JSON JValue.
   *
   * @param node A JValue representing an array of enrichment JSONs
   * @param localMode Whether to use the local MaxMind data file
   *        Enabled for tests
   * @param resolver (implicit) The Iglu resolver used for
   *        schema lookup and validation
   * @return Validation boxing an EnrichmentRegistry object
   *         containing enrichments configured from node
   * @todo remove all the JsonNode round-tripping when
   *       we have ValidatableJValue
   */
  def parse(node: JValue, localMode: Boolean)(implicit resolver: Resolver): ValidatedNelMessage[EnrichmentRegistry] = {

    // Check schema, validate against schema, convert to List[JValue]
    val enrichments: ValidatedNelMessage[List[JValue]] = for {
      d <- asJsonNode(node).verifySchemaAndValidate(EnrichmentConfigSchemaCriterion, true)
    } yield
      (fromJsonNode(d) match {
        case JArray(x) => x
        case _ =>
          throw new Exception(
            "Enrichments JSON not an array - the enrichments JSON schema should prevent this happening")
      })

    // Check each enrichment validates against its own schema
    val configs: ValidatedNelMessage[EnrichmentMap] = (for {
      jsons <- enrichments
    } yield
      for {
        json <- jsons
      } yield
        for {
          pair <- asJsonNode(json).validateAndIdentifySchema(dataOnly = true)
          conf <- buildEnrichmentConfig(pair._1, fromJsonNode(pair._2), localMode)
        } yield conf)
      .flatMap(_.sequenceU) // Swap nested List[scalaz.Validation[...]
      .map(_.flatten.toMap) // Eliminate our Option boxing (drop Nones)

    // Build an EnrichmentRegistry from the Map
    configs.bimap(e => NonEmptyList(e.toString.toProcessingMessage), s => EnrichmentRegistry(s))
  }

  /**
   * Builds an Enrichment from a JValue if it has a
   * recognized name field and matches a schema key
   *
   * @param enrichmentConfig JValue with enrichment information
   * @param schemaKey SchemaKey for the JValue
   * @param localMode Whether to use the local MaxMind data file
   *        Enabled for tests
   * @return ValidatedNelMessage boxing Option boxing Tuple2 containing
   *         the Enrichment object and the schemaKey
   */
  private def buildEnrichmentConfig(
    schemaKey: SchemaKey,
    enrichmentConfig: JValue,
    localMode: Boolean
  ): ValidatedNelMessage[Option[Tuple2[String, Enrichment]]] = {
    val enabled = ScalazJson4sUtils.extract[Boolean](enrichmentConfig, "enabled").toValidationNel
    enabled match {
      case Success(false) => None.success.toValidationNel // Enrichment is disabled
      case _ =>
        val name = ScalazJson4sUtils.extract[String](enrichmentConfig, "name").toValidationNel
        name.flatMap(nm =>
          nm match {
            case "ip_lookups" =>
              IpLookupsEnrichment.parse(enrichmentConfig, schemaKey, localMode).map((nm, _).some)
            case "anon_ip" =>
              AnonIpEnrichment.parse(enrichmentConfig, schemaKey).map((nm, _).some)
            case "referer_parser" =>
              RefererParserEnrichment.parse(enrichmentConfig, schemaKey).map((nm, _).some)
            case "campaign_attribution" =>
              CampaignAttributionEnrichment.parse(enrichmentConfig, schemaKey).map((nm, _).some)
            case "user_agent_utils_config" =>
              UserAgentUtilsEnrichmentConfig.parse(enrichmentConfig, schemaKey).map((nm, _).some)
            case "ua_parser_config" =>
              UaParserEnrichmentConfig.parse(enrichmentConfig, schemaKey).map((nm, _).some)
            case "yauaa_enrichment_config" =>
              YauaaEnrichment.parse(enrichmentConfig, schemaKey).map((nm, _).some)
            case "currency_conversion_config" =>
              CurrencyConversionEnrichmentConfig.parse(enrichmentConfig, schemaKey).map((nm, _).some)
            case "javascript_script_config" =>
              JavascriptScriptEnrichmentConfig.parse(enrichmentConfig, schemaKey).map((nm, _).some)
            case "event_fingerprint_config" =>
              EventFingerprintEnrichmentConfig.parse(enrichmentConfig, schemaKey).map((nm, _).some)
            case "cookie_extractor_config" =>
              CookieExtractorEnrichmentConfig.parse(enrichmentConfig, schemaKey).map((nm, _).some)
            case "http_header_extractor_config" =>
              HttpHeaderExtractorEnrichmentConfig.parse(enrichmentConfig, schemaKey).map((nm, _).some)
            case "weather_enrichment_config" =>
              WeatherEnrichmentConfig.parse(enrichmentConfig, schemaKey).map((nm, _).some)
            case "api_request_enrichment_config" =>
              ApiRequestEnrichmentConfig.parse(enrichmentConfig, schemaKey).map((nm, _).some)
            case "sql_query_enrichment_config" =>
              SqlQueryEnrichmentConfig.parse(enrichmentConfig, schemaKey).map((nm, _).some)
            case "pii_enrichment_config" =>
              PiiPseudonymizerEnrichment.parse(enrichmentConfig, schemaKey).map((nm, _).some)
            case "iab_spiders_and_robots_enrichment" =>
              IabEnrichment.parse(enrichmentConfig, schemaKey, localMode).map((nm, _).some)
            case _ =>
              None.success.toValidationNel // Enrichment is not recognized yet
        })
    }
  }
}

/**
 * A registry to hold all of our enrichment
 * configurations.
 *
 * In the future this may evolve to holding
 * all of our enrichments themselves.
 *
 * @param configs Map whose keys are enrichment
 *        names and whose values are the
 *        corresponding enrichment objects
 */
case class EnrichmentRegistry(private val configs: EnrichmentMap) {

  /**
   * A list of all files required by enrichments in the registry.
   * This is specified as a pair with the first element providing the
   * source location of the file and the second indicating the expected
   * local path.
   */
  val filesToCache: List[(URI, String)] =
    configs.values.flatMap(_.filesToCache).toList

  /**
   * Returns an Option boxing the AnonIpEnrichment
   * config value if present, or None if not
   *
   * @return Option boxing the AnonIpEnrichment instance
   */
  def getAnonIpEnrichment: Option[AnonIpEnrichment] =
    getEnrichment[AnonIpEnrichment]("anon_ip")

  /**
   * Returns an Option boxing the IpLookupsEnrichment
   * config value if present, or None if not
   *
   * @return Option boxing the IpLookupsEnrichment instance
   */
  def getIpLookupsEnrichment: Option[IpLookupsEnrichment] =
    getEnrichment[IpLookupsEnrichment]("ip_lookups")

  /**
   * Returns an Option boxing the RefererParserEnrichment
   * config value if present, or None if not
   *
   * @return Option boxing the RefererParserEnrichment instance
   */
  def getRefererParserEnrichment: Option[RefererParserEnrichment] =
    getEnrichment[RefererParserEnrichment]("referer_parser")

  /**
   * Returns an Option boxing the CampaignAttributionEnrichment
   * config value if present, or None if not
   *
   * @return Option boxing the CampaignAttributionEnrichment instance
   */
  def getCampaignAttributionEnrichment: Option[CampaignAttributionEnrichment] =
    getEnrichment[CampaignAttributionEnrichment]("campaign_attribution")

  /**
   * Returns an Option boxing the CurrencyConversionEnrichment
   * config value if present, or None if not
   *
   * @return Option boxing the CurrencyConversionEnrichment instance
   */
  def getCurrencyConversionEnrichment: Option[CurrencyConversionEnrichment] =
    getEnrichment[CurrencyConversionEnrichment]("currency_conversion_config")

  /**
   * Returns an Option boxing the UserAgentUtilsEnrichment
   * config value if present, or None if not
   *
   * @return Option boxing the UserAgentUtilsEnrichment instance
   */
  def getUserAgentUtilsEnrichment: Option[UserAgentUtilsEnrichment.type] =
    getEnrichment[UserAgentUtilsEnrichment.type]("user_agent_utils_config")

  /**
   * Returns an Option boxing the UaParserEnrichment
   * config value if present, or None if not
   *
   * @return Option boxing the UaParserEnrichment instance
   */
  def getUaParserEnrichment: Option[UaParserEnrichment] =
    getEnrichment[UaParserEnrichment]("ua_parser_config")

  /**
   * If the JSON with the config for the enrichment is present,
   * returns the instance of [[YauaaEnrichment]] case class,
   * already instantiated in [[EnrichmentRegistry.buildEnrichmentConfig]].
   * If no config exists for the enrichment, [[None]] is returned.
   */
  def getYauaaEnrichment: Option[YauaaEnrichment] =
    getEnrichment[YauaaEnrichment]("yauaa_enrichment_config")

  /**
   * Returns an Option boxing the JavascriptScriptEnrichment
   * config value if present, or None if not
   *
   * @return Option boxing the JavascriptScriptEnrichment instance
   */
  def getJavascriptScriptEnrichment: Option[JavascriptScriptEnrichment] =
    getEnrichment[JavascriptScriptEnrichment]("javascript_script_config")

  /**
   * Returns an Option boxing the getEventFingerprintEnrichment
   * config value if present, or None if not
   *
   * @return Option boxing the getEventFingerprintEnrichment instance
   */
  def getEventFingerprintEnrichment: Option[EventFingerprintEnrichment] =
    getEnrichment[EventFingerprintEnrichment]("event_fingerprint_config")

  /*
   * Returns an Option boxing the CookieExtractorEnrichment
   * config value if present, or None if not
   *
   * @return Option boxing the CookieExtractorEnrichment instance
   */
  def getCookieExtractorEnrichment: Option[CookieExtractorEnrichment] =
    getEnrichment[CookieExtractorEnrichment]("cookie_extractor_config")

  /*
   * Returns an Option boxing the HttpHeaderExtractorEnrichment
   * config value if present, or None if not
   *
   * @return Option boxing the HttpHeaderExtractorEnrichment instance
   */
  def getHttpHeaderExtractorEnrichment: Option[HttpHeaderExtractorEnrichment] =
    getEnrichment[HttpHeaderExtractorEnrichment]("http_header_extractor_config")

  /**
   * Returns an Option boxing the WeatherEnrichment
   * config value if present, or None if not
   *
   * @return Option boxing the WeatherEnrichment instance
   */
  def getWeatherEnrichment: Option[WeatherEnrichment] =
    getEnrichment[WeatherEnrichment]("weather_enrichment_config")

  /**
   * Returns an Option boxing the ApiRequestEnrichment
   * config value if present, or None if not
   *
   * @return Option boxing the ApiRequestEnrichment instance
   */
  def getApiRequestEnrichment: Option[ApiRequestEnrichment] =
    getEnrichment[ApiRequestEnrichment]("api_request_enrichment_config")

  /**
   * Returns an Option boxing the SqlQueryEnrichment
   * config value if present, or None if not
   *
   * @return Option boxing the SqlQueryEnrichment instance
   */
  def getSqlQueryEnrichment: Option[SqlQueryEnrichment] =
    getEnrichment[SqlQueryEnrichment]("sql_query_enrichment_config")

  /**
   * Returns an Option boxing the PiiPseudonymizerEnrichment
   * config value if present, or None if not
   *
   * @return Option boxing the PiiPseudonymizerEnrichment instance
   */
  def getPiiPseudonymizerEnrichment: Option[PiiPseudonymizerEnrichment] =
    getEnrichment[PiiPseudonymizerEnrichment]("pii_enrichment_config")

  /**
   * Returns an Option boxing the IabEnrichment
   * config value if present, or None if not
   *
   * @return Option boxing the IabEnrichment instance
   */
  def getIabEnrichment: Option[IabEnrichment] =
    getEnrichment[IabEnrichment]("iab_spiders_and_robots_enrichment")

  /**
   * Returns an Option boxing an Enrichment
   * config value if present, or None if not
   *
   * @tparam A Expected type of the enrichment to get
   * @param name The name of the enrichment to get
   * @return Option boxing the enrichment
   */
  private def getEnrichment[A <: Enrichment: Manifest](name: String): Option[A] =
    configs.get(name).map(cast[A](_))

  /**
   * Adapted from http://stackoverflow.com/questions/6686992/scala-asinstanceof-with-parameterized-types
   * Used to convert an Enrichment to a
   * specific subtype of Enrichment
   *
   * @tparam A Type to cast to
   * @param a The object to cast to type A
   * @return a, converted to type A
   */
  private def cast[A <: AnyRef: Manifest](a: Any): A =
    manifest.runtimeClass.cast(a).asInstanceOf[A]
}
