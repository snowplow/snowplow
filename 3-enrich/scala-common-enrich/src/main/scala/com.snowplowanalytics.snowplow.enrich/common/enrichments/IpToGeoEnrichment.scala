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
package enrichments

import config._
import utils.ScalazJson4sUtils

// Java
import java.net.URI

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

// Scala MaxMind GeoIP
import com.snowplowanalytics.maxmind.geoip.{IpGeo, IpLocation}

/**
* Companion object. Lets us create a IpToGeoEnrichment
* from a JValue.
*/
object IpToGeoEnrichment extends EnrichmentConfigParseable {

  val supportedSchemaKey = SchemaKey("com.snowplowanalytics.snowplow", "ip_to_geo", "jsonschema", "1-0-0")

  /**
   * Creates an IpToGeoEnrichment instance from a JValue.
   * 
   * @param config The ip_to_geo enrichment JSON
   * @return a configured IpToGeoEnrichment instance
   */
  def parse(config: JValue, schemaKey: SchemaKey): ValidationNel[String, IpToGeoEnrichment] = {
    isParseable(config, schemaKey).flatMap( conf => {
      val geoUri = ScalazJson4sUtils.extractString(conf, parameter("maxmindUri"))
      val geoDb  = ScalazJson4sUtils.extractString(conf, parameter("maxmindDatabase"))
      
      (geoUri.toValidationNel |@| geoDb.toValidationNel) {
        IpToGeoEnrichment(_, _)
      }
    })
  }

}

/**
 * Contains enrichments related to geo-location.
 */
case class IpToGeoEnrichment(
  maxmindUri: String,
  maxmindDatabase: String
  ) extends EnrichmentConfig {

  lazy val ipGeo = createIpGeo(maxmindUri + maxmindDatabase)

  /**
   * A helper to create the new IpGeo object.
   *
   * @param ipGeoFile The path to the MaxMind GeoLiteCity.dat file
   * @return an IpGeo object ready to perform IP->geo lookups
   */
  private def createIpGeo(ipGeoFile: String): IpGeo =
    IpGeo(ipGeoFile, memCache = true, lruCache = 20000)

  /**
   * Returns the URI of the Maxmind database
   *
   * @return URI of the Maxmind database
   */
  def getRemotePath: URI =
    getClass.getResource(maxmindUri + "/" + maxmindDatabase).toURI

  /**
   * Returns the filepath of the local database.
   * Used for testing only.
   *
   * @return Filepath of the mock Maxmind database
   */
  def getLocalPath: String =
    getClass.getResource("/maxmind/" + maxmindDatabase).toURI.getPath

  /**
   * Extract the geo-location using the
   * client IP address.
   *
   * Note we wrap the getLocation call in a try
   * catch block. At the time of writing, no
   * valid or invalid IP address can make
   * getLocation throw an Exception, but we keep
   * this protection in case this situation
   * changes in the future (as we don't control
   * the functionality of the underlying MaxMind
   * Java API).
   *
   * @param geo The IpGeo lookup engine we will
   *        use to lookup the client's IP address
   * @param ip The client's IP address to use to
   *        lookup the client's geo-location
   * @return a MaybeIpLocation (Option-boxed
   *         IpLocation), or an error message,
   *         boxed in a Scalaz Validation
   */
  // TODO: can we move the IpGeo to an implicit?
  def extractGeoLocation(ip: String): Validation[String, MaybeIpLocation] = {

    try {
      ipGeo.getLocation(ip).success
    } catch {
      case _ => return "Could not extract geo-location from IP address [%s]".format(ip).fail
    }
  }
}
