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
package utils

// Scalaz
import scalaz._
import Scalaz._

// json4s
import org.json4s.scalaz.JsonScalaz._
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

trait EnrichmentConfig

/**
* Companion object. Lets us create a IpToGeoEnrichmentConfig
* from a JValue.
*/
object IpToGeoEnrichmentConfig {

  /**
   * Creates an IpToGeoEnrichmentConfig instance from a JValue.
   * 
   * @param config The ip_to_geo enrichment JSON
   * @return a configured IpToGeoEnrichmentConfig instance
   */
  def parse(config: JValue): ValidationNel[String, IpToGeoEnrichmentConfig] = {
    val geoDb  = ScalazJson4sUtils.extractString(config, NonEmptyList("parameters", "maxmindDatabase"))
    val geoUri = ScalazJson4sUtils.extractString(config, NonEmptyList("parameters", "maxmindUri"))
    
    (geoDb.toValidationNel |@| geoUri.toValidationNel) {
      IpToGeoEnrichmentConfig(_, _)
    }
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
object AnonIpEnrichmentConfig extends EnrichmentConfig {

  implicit val formats = DefaultFormats

  /**
   * Creates an AnonIpEnrichmentConfig instance from a JValue.
   * 
   * @param config The anon_ip enrichment JSON
   * @return a configured AnonIpEnrichmentConfig instance
   */
  def parse(config: JValue): ValidationNel[String, AnonIpEnrichmentConfig] = {
    val anonOctets = ScalazJson4sUtils.extractInt(config, NonEmptyList("parameters", "anonOctets"))

    (anonOctets).bimap(
      e => NonEmptyList(e.toString),
      s => AnonIpEnrichmentConfig(s)
    )
  }
  
}

/**
 * Config for an anon_ip enrichment
 */
case class AnonIpEnrichmentConfig(
  anonOctets: Int
  ) extends EnrichmentConfig
