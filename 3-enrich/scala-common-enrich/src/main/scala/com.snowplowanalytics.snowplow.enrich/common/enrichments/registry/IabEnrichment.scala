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

import java.io.File
import java.net.{InetAddress, URI, UnknownHostException}

import com.snowplowanalytics.iglu.client.validation.ProcessingMessageMethods._
import com.snowplowanalytics.iglu.client.{SchemaCriterion, SchemaKey}
import com.snowplowanalytics.iab.spidersandrobotsclient.IabClient
import org.joda.time.DateTime
import org.json4s.{DefaultFormats, Extraction, JObject, JValue}
import org.json4s.JsonDSL._
import scalaz._
import Scalaz._

import utils.{ConversionUtils, ScalazJson4sUtils}

/**
 * Companion object. Lets us create an IabEnrichment
 * instance from a JValue.
 */
object IabEnrichment extends ParseableEnrichment {

  implicit val formats = DefaultFormats

  val supportedSchema = SchemaCriterion(
    "com.snowplowanalytics.snowplow.enrichments",
    "iab_spiders_and_robots_enrichment",
    "jsonschema",
    1,
    0)

  /**
   * Creates an IabEnrichment instance from a JValue.
   *
   * @param config    The iab_spiders_and_robots_enrichment JSON
   * @param schemaKey The SchemaKey provided for the enrichment
   *                  Must be a supported SchemaKey for this enrichment
   * @param localMode Whether to use the local IAB database file
   *                  Enabled for tests
   * @return a configured IabEnrichment instance
   */
  def parse(config: JValue, schemaKey: SchemaKey, localMode: Boolean): ValidatedNelMessage[IabEnrichment] =
    isParseable(config, schemaKey).flatMap { conf =>
      def uri(name: String) = getIabDbFromName(conf, name).sequenceU

      def string(name: String) = getStringFromName(conf, name).sequenceU

      (uri("ipFile") |@| uri("excludeUseragentFile") |@| uri("includeUseragentFile")) {
        IabEnrichment(_, _, _, localMode)
      }
    }

  /**
   * Creates IabDatabase instances used in the IabEnrichment case class.
   *
   * @param config The iab_spiders_and_robots_enrichment JSON
   * @param name   The name of the field.
   *               e.g. "ipFile", "excluseUseragentFile", "includeUseragentFile"
   * @return None if the field does not exist,
   *         Some(Failure) if the URI is invalid,
   *         Some(Success) if it is found
   */
  private def getIabDbFromName(config: JValue, name: String): Option[ValidatedNelMessage[IabDatabase]] =
    if (ScalazJson4sUtils.fieldExists(config, "parameters", name)) {
      val uri = ScalazJson4sUtils.extract[String](config, "parameters", name, "uri")
      val db = ScalazJson4sUtils.extract[String](config, "parameters", name, "database")

      (uri.toValidationNel |@| db.toValidationNel) { (uri, db) =>
        getDatabaseUri(uri, db).toValidationNel.map[IabDatabase](u => IabDatabase(name, u, db))
      }.flatMap(identity).some

    } else None

  /**
   * Extracts simple string fields from an enrichment JSON.
   *
   * @param config The iab_spiders_and_robots_enrichment JSON
   * @param name   The name of the field.
   *               e.g. "ipFile", "excluseUseragentFile", "includeUseragentFile"
   * @return None if the field does not exist,
   *         Some(Success) if it is found
   */
  private def getStringFromName(config: JValue, name: String): Option[ValidatedNelMessage[String]] =
    if (ScalazJson4sUtils.fieldExists(config, "parameters", name)) {
      ScalazJson4sUtils.extract[String](config, "parameters", name).toValidationNel.some
    } else None

  /**
   * Convert the path to the IAB file from a
   * String to a Validation[URI].
   *
   * @param uri      URI to the IAB database file
   * @param database Name of the IAB database
   * @return a Validation-boxed URI
   */
  private def getDatabaseUri(uri: String, database: String): ValidatedMessage[URI] =
    ConversionUtils
      .stringToUri(uri + (if (uri.endsWith("/")) "" else "/") + database)
      .flatMap(_ match {
        case Some(u) => u.success
        case None => "URI to IAB file must be provided".fail
      })
      .toProcessingMessage
}

/**
 * Contains enrichments based on IAB Spiders&Robots lookup.
 *
 * @param ipFile             (Full URI to the IAB excluded IP list, database name)
 * @param excludeUaFile      (Full URI to the IAB excluded user agent list, database name)
 * @param includeUaFile      (Full URI to the IAB included user agent list, database name)
 * @param localMode          Whether to use the local database file. Enabled for tests.
 */
case class IabEnrichment(
  ipFile: Option[IabDatabase],
  excludeUaFile: Option[IabDatabase],
  includeUaFile: Option[IabDatabase],
  localMode: Boolean
) extends Enrichment {

  private type DbEntry = Option[(Option[URI], String)]

  private val schemaUri = "iglu:com.iab.snowplow/spiders_and_robots/jsonschema/1-0-0"
  private implicit val formats = DefaultFormats

  // Construct a Tuple3 of all IAB files
  private val dbs: (DbEntry, DbEntry, DbEntry) = {

    def db(iabDb: Option[IabDatabase]): DbEntry = iabDb.map {
      case IabDatabase(name, uri, db) =>
        if (localMode) {
          (None, getClass.getResource(db).toURI.getPath)
        } else {
          (Some(uri), "./iab_" + name)
        }
    }

    (db(ipFile), db(excludeUaFile), db(includeUaFile))
  }

  // Collect a cache of IAB files for local download
  override val filesToCache: List[(URI, String)] =
    (dbs._1 ++ dbs._2 ++ dbs._3).collect {
      case (Some(uri), path) => (uri, path)
    }.toList

  // Create an IAB client based on the IAB files list
  private lazy val iabClient = {
    def file(db: DbEntry): File = new File(db.get._2)

    new IabClient(file(dbs._1), file(dbs._2), file(dbs._3))
  }

  /**
   * Get the IAB response containing information about whether an event is a
   * spider or robot using the IAB client library.
   *
   * @param userAgent  User agent used to perform the check
   * @param ipAddress  IP address used to perform the check
   * @param accurateAt Date of the event, used to determine whether entries in the
   *                   IAB list are relevant or outdated
   * @return an IabResponse object
   */
  private[enrichments] def performCheck(
    userAgent: String,
    ipAddress: String,
    accurateAt: DateTime): Validation[String, IabEnrichmentResponse] =
    try {
      val result = iabClient.checkAt(userAgent, InetAddress.getByName(ipAddress), accurateAt.toDate)
      IabEnrichmentResponse(
        result.isSpiderOrRobot,
        result.getCategory.toString,
        result.getReason.toString,
        result.getPrimaryImpact.toString).success
    } catch {
      case exc: UnknownHostException => s"IP address $ipAddress was invald".failure
    }

  /**
   * Get the IAB response as a JSON context for a specific event
   *
   * @param userAgent  enriched event optional user agent
   * @param ipAddress  enriched event optional IP address
   * @param accurateAt enriched event optional datetime
   * @return IAB response as a self-describing JSON object
   */
  def getIabContext(
    userAgent: Option[String],
    ipAddress: Option[String],
    accurateAt: Option[DateTime]): Validation[String, JObject] =
    getIab(userAgent, ipAddress, accurateAt).map(addSchema)

  /**
   * Get IAB check response received from the client library and extracted as a JSON object
   *
   * @param userAgent enriched event optional user agent
   * @param ipAddress enriched event optional IP address
   * @param time      enriched event optional datetime
   * @return IAB response as JSON object
   */
  private def getIab(
    userAgent: Option[String],
    ipAddress: Option[String],
    time: Option[DateTime]): Validation[String, JObject] =
    (userAgent, ipAddress, time) match {
      case (Some(ua), Some(ip), Some(t)) =>
        performCheck(ua, ip, t) match {
          case Success(response) =>
            Extraction.decompose(response) match {
              case obj: JObject => obj.success
              case _ => s"Couldn't transform IAB response $response into JSON".failure
            }
          case Failure(message) => message.failure
        }
      case _ =>
        s"One of required event fields missing. user agent: $userAgent, ip address: $ipAddress, time: $time".failure
    }

  /**
   * Add Iglu URI to JSON Object
   *
   * @param context IAB context as JSON Object
   * @return JSON Object wrapped as Self-describing JSON
   */
  private def addSchema(context: JObject): JObject =
    ("schema", schemaUri) ~ (("data", context))
}

/**
 * Case class copy of `com.snowplowanalytics.iab.spidersandrobotsclient.IabResponse`
 */
private[enrichments] case class IabEnrichmentResponse(
  spiderOrRobot: Boolean,
  category: String,
  reason: String,
  primaryImpact: String)

/**
 * Case class representing an IAB database location
 */
case class IabDatabase(name: String, uri: URI, db: String)
