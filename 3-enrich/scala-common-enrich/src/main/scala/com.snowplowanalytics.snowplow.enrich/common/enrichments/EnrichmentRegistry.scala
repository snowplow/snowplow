/*
 * Copyright (c) 2012-2014 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics
package snowplow
package enrich
package common
package enrichments

// Scalaz
import scalaz._
import Scalaz._

// json4s
import org.json4s.scalaz.JsonScalaz._
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

// Iglu
import iglu.client.{
  SchemaKey,
  SchemaCriterion,
  Resolver
}
import iglu.client.validation.ValidatableJsonMethods._
import iglu.client.validation.ProcessingMessageMethods._
import iglu.client.validation.ValidatableJsonNode

// This project
import registry.{
  Enrichment,
  AnonIpEnrichment,
  IpLookupsEnrichment,
  RefererParserEnrichment,
  CampaignAttributionEnrichment,
  UserAgentUtilsEnrichment,
  UaParserEnrichment,
  CurrencyConversionEnrichment,
  JavascriptScriptEnrichment,
  EventFingerprintEnrichment,
  UserAgentUtilsEnrichmentConfig,
  UaParserEnrichmentConfig,
  CurrencyConversionEnrichmentConfig,
  JavascriptScriptEnrichmentConfig,
  EventFingerprintEnrichmentConfig
}

import utils.ScalazJson4sUtils

/**
 * Companion which holds a constructor
 * for the EnrichmentRegistry.
 */
object EnrichmentRegistry {

  private val EnrichmentConfigSchemaCriterion = SchemaCriterion("com.snowplowanalytics.snowplow", "enrichments", "jsonschema", 1, 0)

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
  def parse(node: JValue, localMode: Boolean)(implicit resolver: Resolver): ValidatedNelMessage[EnrichmentRegistry] =  {

    // Check schema, validate against schema, convert to List[JValue]
    val enrichments: ValidatedNelMessage[List[JValue]] = for {
        d <- asJsonNode(node).verifySchemaAndValidate(EnrichmentConfigSchemaCriterion, true)
      } yield (fromJsonNode(d) match {
        case JArray(x) => x
        case _ => throw new Exception("Enrichments JSON not an array - the enrichments JSON schema should prevent this happening")
      })

    // Check each enrichment validates against its own schema
    val configs: ValidatedNelMessage[EnrichmentMap] = (for {
        jsons <- enrichments
      } yield for {    
        json  <- jsons
      } yield for {
        pair  <- asJsonNode(json).validateAndIdentifySchema(dataOnly = true)
        conf  <- buildEnrichmentConfig(pair._1, fromJsonNode(pair._2), localMode)
      } yield conf)
      .flatMap(_.sequenceU) // Swap nested List[scalaz.Validation[...]
      .map(_.flatten.toMap) // Eliminate our Option boxing (drop Nones)

    // Build an EnrichmentRegistry from the Map
    configs.bimap(
      e => NonEmptyList(e.toString.toProcessingMessage),
      s => EnrichmentRegistry(s))
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
  private def buildEnrichmentConfig(schemaKey: SchemaKey, enrichmentConfig: JValue, localMode: Boolean): ValidatedNelMessage[Option[Tuple2[String, Enrichment]]] = {

    val enabled = ScalazJson4sUtils.extract[Boolean](enrichmentConfig, "enabled").toValidationNel
        
    enabled match {
      case Success(false) => None.success.toValidationNel // Enrichment is disabled
      case e => {
        val name = ScalazJson4sUtils.extract[String](enrichmentConfig, "name").toValidationNel
        name.flatMap( nm => {

          if (nm == "ip_lookups") {
            IpLookupsEnrichment.parse(enrichmentConfig, schemaKey, localMode).map((nm, _).some)
          } else if (nm == "anon_ip") {
            AnonIpEnrichment.parse(enrichmentConfig, schemaKey).map((nm, _).some)
          } else if (nm == "referer_parser") {
            RefererParserEnrichment.parse(enrichmentConfig, schemaKey).map((nm, _).some)            
          } else if (nm == "campaign_attribution") {
            CampaignAttributionEnrichment.parse(enrichmentConfig, schemaKey).map((nm, _).some)            
          } else if (nm == "user_agent_utils_config") {
            UserAgentUtilsEnrichmentConfig.parse(enrichmentConfig, schemaKey).map((nm, _).some)
          } else if (nm == "ua_parser_config") {
            UaParserEnrichmentConfig.parse(enrichmentConfig, schemaKey).map((nm, _).some)
          } else if (nm == "currency_conversion_config") {
            CurrencyConversionEnrichmentConfig.parse(enrichmentConfig, schemaKey).map((nm, _).some)
          } else if (nm == "javascript_script_config") {
            JavascriptScriptEnrichmentConfig.parse(enrichmentConfig, schemaKey).map((nm, _).some)
          } else if (nm == "event_fingerprint_config") {
            EventFingerprintEnrichmentConfig.parse(enrichmentConfig, schemaKey).map((nm, _).some)
          } else {
            None.success // Enrichment is not recognized yet
          }
        })
      }
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
  def getUaParserEnrichment: Option[UaParserEnrichment.type] = 
    getEnrichment[UaParserEnrichment.type]("ua_parser_config")

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
  private def cast[A <: AnyRef : Manifest](a : Any) : A =
    manifest.runtimeClass.cast(a).asInstanceOf[A]
}
