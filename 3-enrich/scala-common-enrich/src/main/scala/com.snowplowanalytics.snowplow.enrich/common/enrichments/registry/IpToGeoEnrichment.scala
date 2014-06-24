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
package registry

// Java
import java.net.URI

// Scalaz
import scalaz._
import Scalaz._

// json4s
import org.json4s.JValue

// Iglu
import iglu.client.SchemaKey
import iglu.client.validation.ProcessingMessageMethods._

// Scala MaxMind GeoIP
import maxmind.geoip.{
  IpGeo,
  IpLocation
}

// This project
import common.utils.ConversionUtils
import utils.ScalazJson4sUtils

/**
* Companion object. Lets us create a IpToGeoEnrichment
* from a JValue.
*/
object IpToGeoEnrichment extends ParseableEnrichment {

  val supportedSchemaKey = SchemaKey("com.snowplowanalytics.snowplow", "ip_to_geo", "jsonschema", "1-0-0")

  /**
   * Creates an IpToGeoEnrichment instance from a JValue.
   * 
   * @param config The ip_to_geo enrichment JSON
   * @param schemaKey The SchemaKey provided for the enrichment
   *        Must be a supported SchemaKey for this enrichment
   * @param localMode Whether to use the local MaxMind data file
   *        Enabled for tests
   * @return a configured IpToGeoEnrichment instance
   */
  def parse(config: JValue, schemaKey: SchemaKey, localMode: Boolean): ValidatedNelMessage[IpToGeoEnrichment] = {
    isParseable(config, schemaKey).flatMap( conf => {
      val geoUri = ScalazJson4sUtils.extractString(conf, parameter("maxmindUri"))
      val geoDb  = ScalazJson4sUtils.extractString(conf, parameter("maxmindDatabase"))
      
      (geoUri.toValidationNel |@| geoDb.toValidationNel) { (uri, db) =>
        for {
          u <- (getMaxmindUri(uri, db).toValidationNel: ValidatedNelMessage[URI])
          e =  IpToGeoEnrichment(u, db, localMode)
        } yield e
      }.flatMap(x => x) // No flatten in Scalaz
    })
  }

  /**
   * Convert the Maxmind file from a
   * String to a Validation[URI].
   *
   * @param maxmindFile A String holding the
   *        URI to the hosted MaxMind file
   * @param database Name of the MaxMind
   *        database
   * @return a Validation-boxed URI
   */
  private def getMaxmindUri(uri: String, database: String): ValidatedMessage[URI] =
    ConversionUtils.stringToUri(uri + "/" + database).flatMap(_ match {
      case Some(u) => u.success
      case None => "URI to MaxMind file must be provided".fail
      }).toProcessingMessage

}

/**
 * Contains enrichments related to geo-location.
 *
 * @param uri Full URI to the MaxMind data file
 * @param database Name of the MaxMind database
 * @param localMode Whether to use the local
 *        MaxMind data file. Enabled for tests.
 */
case class IpToGeoEnrichment(
  uri: URI,
  database: String,
  localMode: Boolean
  ) extends Enrichment {

  // Checked in Hadoop Enrich to decide whether to copy to
  // the Hadoop dist cache or not
  val cachePath = if (!localMode) "./geoip".some else None

  private lazy val MaxmindResourcePath =
    getClass.getResource("/maxmind/" + database).toURI.getPath

  // Initialize our IpGeo object. Hopefully the database
  // has been copied to our cache path by Hadoop Enrich 
  val ipGeo = {
    val path = cachePath.getOrElse(MaxmindResourcePath)
    IpGeo(path, memCache = true, lruCache = 20000)
  }

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
      case _: Throwable => return "Could not extract geo-location from IP address [%s]".format(ip).fail
    }
  }
}
