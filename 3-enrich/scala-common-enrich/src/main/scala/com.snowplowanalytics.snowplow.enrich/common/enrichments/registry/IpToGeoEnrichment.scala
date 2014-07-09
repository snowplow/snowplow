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

// Maven Artifact
import org.apache.maven.artifact.versioning.DefaultArtifactVersion

// Scalaz
import scalaz._
import Scalaz._

// json4s
import org.json4s.JValue

// Iglu
import iglu.client.SchemaKey
import iglu.client.validation.ProcessingMessageMethods._

// Scala MaxMind GeoIP
import maxmind.iplookups.{
  IpLookups,
  IpLookupResult
}

// This project
import common.utils.ConversionUtils
import utils.ScalazJson4sUtils

/**
* Companion object. Lets us create a IpToGeoEnrichment
* from a JValue.
*/
object IpLookupsEnrichment extends ParseableEnrichment {

  val supportedSchemaKey = SchemaKey("com.snowplowanalytics.snowplow", "ip_to_geo", "jsonschema", "1-0-0")

  private val lookupNames = List("geo", "isp", "organization", "domain")

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
  def parse(config: JValue, schemaKey: SchemaKey, localMode: Boolean): ValidatedNelMessage[IpLookupsEnrichment] = {

    isParseable(config, schemaKey).flatMap( conf => {

      val argsList: List[Option[ValidatedNelMessage[(URI, String)]]] = List("geo", "isp", "org", "domain").map(getArgumentFromName(conf,_))

      val switchedArgsList: List[ValidatedNelMessage[Option[(URI, String)]]] = argsList.map(x => {
        x match {
          case None => None.success.toValidationNel
          case Some(Failure(f)) => f.fail
          case Some(Success(s)) => Some(s).success.toValidationNel
        }
      })
      (switchedArgsList(0) |@| switchedArgsList(1) |@| switchedArgsList(2) |@| switchedArgsList(3)) { IpLookupsEnrichment(_,_,_,_, localMode) }
    })
  }


  // TODO: docstring
  private def getArgumentFromName(conf: JValue, name: String): Option[ValidatedNelMessage[(URI, String)]] = {
    if (ScalazJson4sUtils.fieldExists(conf, "parameters", name)) {
      val uri = ScalazJson4sUtils.extract[String](conf, "parameters", name, "uri")
      val db  = ScalazJson4sUtils.extract[String](conf, "parameters", name, "database")

      Some((uri.toValidationNel |@| db.toValidationNel) { (uri, db) =>
        for {
          u <- (getMaxmindUri(uri, db).toValidationNel: ValidatedNelMessage[URI])
        } yield (u, db)

      }.flatMap(x => x))

    } else None
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
 * Contains enrichments based on IP address.
 *
 * @param uri Full URI to the MaxMind data file
 * @param database Name of the MaxMind database

 * @param geoTuple (Full URI to the geo lookup
 *        MaxMind data file, database name)
 * @param geoTuple (Full URI to the ISP lookup
 *        MaxMind data file, database name)
 * @param geoTuple (Full URI to the organization
 *        lookup MaxMind data file
 * @param geoTuple (Full URI to the domain lookup
 *        MaxMind data file, database name)
 * @param localMode Whether to use the local
 *        MaxMind data file. Enabled for tests. 
 */
case class IpLookupsEnrichment(
  geoTuple: Option[(URI, String)],
  ispTuple: Option[(URI, String)],
  orgTuple: Option[(URI, String)],
  domainTuple: Option[(URI, String)],
  localMode: Boolean
  ) extends Enrichment {

  val version = new DefaultArtifactVersion("0.1.0")

  val lookupNames = List("geo", "isp", "organization", "domain")

  val lookupMap: Map[String, (URI, String)] = Map("geo" -> geoTuple, "isp" -> ispTuple, "org" -> orgTuple, "domain" -> domainTuple)
                    .collect{case (key, Some(tuple)) => (key, tuple)}

  private def getCachePath(name: String): Option[String] = if (!localMode) ("./ip_" + name).some else None

  // Checked in Hadoop Enrich to decide whether to copy to
  // the Hadoop dist cache or not
  private val cacheMap = lookupMap.map(kv => (kv._1, getCachePath(kv._1)))

  val lookupPaths = lookupNames.map(lookupName => {
    if (lookupMap.contains(lookupName)) {

      lazy val maxmindResourcePath = 
        getClass.getResource("/maxmind/" + lookupMap(lookupName)._2).toURI.getPath

     // Hopefully the database has been copied to our cache path by Hadoop Enrich 
      val path = cacheMap(lookupName) match {
        case None => Some(maxmindResourcePath)
        case Some(s) => Some(s)
      }
      path
    }
    else None
  })

  val ipLookups = IpLookups(lookupPaths(0), lookupPaths(1), lookupPaths(2), lookupPaths(3), memCache = true, lruCache = 20000)

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
  def extractGeoLocation(ip: String): Validation[String, IpLookupResult] = {

    try {
      ipLookups.performLookups(ip).success
    } catch {
      case _: Throwable => return "Could not extract geo-location from IP address [%s]".format(ip).fail
    }
  }
}
