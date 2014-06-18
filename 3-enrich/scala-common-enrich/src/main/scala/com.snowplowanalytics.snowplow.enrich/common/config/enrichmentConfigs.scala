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
package com.snowplowanalytics.snowplow.enrich.common
package config

import utils.ScalazJson4sUtils

// Scalaz
import scalaz._
import Scalaz._

// json4s
import org.json4s.scalaz.JsonScalaz._
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

// Iglu
import com.snowplowanalytics.iglu.client._

/**
 * Trait inherited by every enrichment config case class
 */
trait EnrichmentConfig

/**
 * Trait to hold helpers relating to enrichment config
 */
trait EnrichmentConfigParseable {

  val supportedSchemaKey: SchemaKey

  /**
   * Tests whether a JSON is parseable by a
   * specific EnrichmentConfig constructor
   *
   * @param config The JSON
   * @param schemaKey The schemaKey which needs
   * to be checked
   * @return The JSON or an error message, boxed
   */
  def isParseable(config: JValue, schemaKey: SchemaKey): ValidationNel[String, JValue] = {
    if (schemaKey == supportedSchemaKey) {
      config.success
    } else {
      ("Wrong type of JSON for an enrichment of type %").format(schemaKey.name).fail.toValidationNel
    }
  }

  /**
   * Shortcut for getting a value from within
   * the "parameters" field of a JSON
   *
   * @param property The name of the field
   * @return NonEmptyList of the inner value
   */
  def parameter(property: String) = NonEmptyList("parameters", property)
}

/**
* Companion object. Lets us create a IpToGeoEnrichmentConfig
* from a JValue.
*/
object IpToGeoEnrichmentConfig extends EnrichmentConfigParseable {

  val supportedSchemaKey = SchemaKey("com.snowplowanalytics.snowplow", "ip_to_geo", "jsonschema", "1-0-0")

  /**
   * Creates an IpToGeoEnrichmentConfig instance from a JValue.
   * 
   * @param config The ip_to_geo enrichment JSON
   * @return a configured IpToGeoEnrichmentConfig instance
   */
  def parse(config: JValue, schemaKey: SchemaKey): ValidationNel[String, IpToGeoEnrichmentConfig] = {
    isParseable(config, schemaKey).flatMap( conf => {
      val geoDb  = ScalazJson4sUtils.extractString(conf, parameter("maxmindDatabase"))
      val geoUri = ScalazJson4sUtils.extractString(conf, parameter("maxmindUri"))
      
      (geoDb.toValidationNel |@| geoUri.toValidationNel) {
        IpToGeoEnrichmentConfig(_, _)
      }
    })
  }

}

/**
 * Config for an ip_to_geo enrichment
 */
case class IpToGeoEnrichmentConfig(
  maxmindDatabase: String,
  maxmindUri: String
  ) extends EnrichmentConfig


/**
* Companion object. Lets us create a AnonIpEnrichmentConfig
* from a JValue.
*/
object AnonIpEnrichmentConfig extends EnrichmentConfigParseable {

  val supportedSchemaKey = SchemaKey("com.snowplowanalytics.snowplow", "anon_ip", "jsonschema", "1-0-0")

  /**
   * Creates an AnonIpEnrichmentConfig instance from a JValue.
   * 
   * @param config The anon_ip enrichment JSON
   * @return a configured AnonIpEnrichmentConfig instance
   */
  def parse(config: JValue, schemaKey: SchemaKey): ValidationNel[String, AnonIpEnrichmentConfig] = {
    isParseable(config, schemaKey).flatMap( conf => {
      val anonOctets = ScalazJson4sUtils.extractInt(config, parameter("anonOctets"))

      (anonOctets).bimap(
        e => NonEmptyList(e.toString),
        s => AnonIpEnrichmentConfig(s)
      )
    })
  }
  
}

/**
 * Config for an anon_ip enrichment
 */
case class AnonIpEnrichmentConfig(
  anonOctets: Int
  ) extends EnrichmentConfig
