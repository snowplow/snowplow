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
import iglu.client.{
  SchemaCriterion,
  SchemaKey
}
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
* Companion object. Lets us create an IpLookupsEnrichment
* instance from a JValue.
*/
object IpLookupsEnrichment extends ParseableEnrichment {

  val supportedSchema = SchemaCriterion("com.snowplowanalytics.snowplow", "ip_lookups", "jsonschema", 1, 0)

  /**
   * Creates an IpLookupsEnrichment instance from a JValue.
   * 
   * @param config The ip_lookups enrichment JSON
   * @param schemaKey The SchemaKey provided for the enrichment
   *        Must be a supported SchemaKey for this enrichment
   * @param localMode Whether to use the local MaxMind data file
   *        Enabled for tests
   * @return a configured IpLookupsEnrichment instance
   */
  def parse(config: JValue, schemaKey: SchemaKey, localMode: Boolean): ValidatedNelMessage[IpLookupsEnrichment] = {

    isParseable(config, schemaKey).flatMap( conf => {
      def db(name: String) = getArgumentFromName(conf, name).sequenceU
      (db("geo") |@| db("isp") |@| db("organization") |@| db("domain") |@| db("netspeed")) { IpLookupsEnrichment(_,_,_,_,_,localMode) }
    })
  }

  /**
   * Creates the (URI, String) tuple arguments
   * which are the case class parameters
   *
   * @param conf The ip_lookups enrichment JSON
   * @param name The name of the lookup:
   *        "geo", "isp", "organization", "domain"
   * @return None if the database isn't being used,
   *         Some(Failure) if its URI is invalid,
   *         Some(Success) if it is found
   */
  private def getArgumentFromName(conf: JValue, name: String): Option[ValidatedNelMessage[(String, URI, String)]] = {

    if (ScalazJson4sUtils.fieldExists(conf, "parameters", name)) {
      val uri = ScalazJson4sUtils.extract[String](conf, "parameters", name, "uri")
      val db  = ScalazJson4sUtils.extract[String](conf, "parameters", name, "database")

      (uri.toValidationNel |@| db.toValidationNel) { (uri, db) =>
        for {
          u <- (getMaxmindUri(uri, db).toValidationNel: ValidatedNelMessage[URI])
        } yield (name, u, db)

      }.flatMap(x => x).some

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
 * @param ispTuple (Full URI to the ISP lookup
 *        MaxMind data file, database name)
 * @param orgTuple (Full URI to the organization
 *        lookup MaxMind data file
 * @param domainTuple (Full URI to the domain lookup
 *        MaxMind data file, database name)
 * @param netspeedTuple (Full URI to the netspeed
 *        lookup MaxMind data file, database name)
 * @param localMode Whether to use the local
 *        MaxMind data file. Enabled for tests. 
 */
case class IpLookupsEnrichment(
  geoTuple: Option[(String, URI, String)],
  ispTuple: Option[(String, URI, String)],
  orgTuple: Option[(String, URI, String)],
  domainTuple: Option[(String, URI, String)],
  netspeedTuple: Option[(String, URI, String)],
  localMode: Boolean
  ) extends Enrichment {

  val version = new DefaultArtifactVersion("0.1.0")

  private type FinalPath = String
  private type DbEntry = Option[(Option[URI], FinalPath)]

  // Construct a Tuple5 of all the IP Lookup databases
  private val dbs: Tuple5[DbEntry, DbEntry, DbEntry, DbEntry, DbEntry] = {

    def db(dbPath: Option[(String, URI, String)]): DbEntry = dbPath.map { case (name, uri, file) =>
      if (localMode) {
        (None, getClass.getResource(file).toURI.getPath)
      } else {
        (Some(uri), "./ip_" + name)
      }
    }

    (db(geoTuple), db(ispTuple), db(orgTuple), db(domainTuple), db(netspeedTuple))
  }

  // Collect the cache paths to install
  val dbsToCache: List[(URI, FinalPath)] =
    (dbs._1 ++ dbs._2 ++ dbs._3 ++ dbs._4 ++ dbs._5).collect {
      case (Some(uri), finalPath) => (uri, finalPath)
    }.toList

  // Must be lazy as we don't have the files copied to
  // the Dist Cache on HDFS yet
  private lazy val ipLookups = {
    def path(db: DbEntry): Option[FinalPath] = db.map(_._2)
    IpLookups(path(dbs._1), path(dbs._2), path(dbs._3), path(dbs._4), path(dbs._5), memCache = true, lruCache = 20000)
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
  def extractIpInformation(ip: String): Validation[String, IpLookupResult] = {

    try {
      ipLookups.performLookups(ip).success
    } catch {
      case _: Throwable => "Could not extract geo-location from IP address [%s]".format(ip).fail
    }
  }
}
