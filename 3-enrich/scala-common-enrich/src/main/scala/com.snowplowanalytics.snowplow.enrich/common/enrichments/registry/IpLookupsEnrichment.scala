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

import java.net.URI

import cats.data.{NonEmptyList, ValidatedNel}
import cats.implicits._
import com.snowplowanalytics.maxmind.iplookups.IpLookups
import com.snowplowanalytics.maxmind.iplookups.model.{IpLocation, IpLookupResult => IpLookupRes}
import com.snowplowanalytics.iglu.core.{SchemaCriterion, SchemaKey}
import io.circe._

import utils.CirceUtils

/** Companion object. Lets us create an IpLookupsEnrichment instance from a Json. */
object IpLookupsEnrichment extends ParseableEnrichment {
  val supportedSchema =
    SchemaCriterion("com.snowplowanalytics.snowplow", "ip_lookups", "jsonschema", 2, 0)

  /**
   * Creates an IpLookupsEnrichment instance from a JValue.
   * @param c The ip_lookups enrichment JSON
   * @param schemaKey provided for the enrichment, must be supported  by this enrichment
   * @param localMode Whether to use the local MaxMind data file, enabled for tests
   * @return a configured IpLookupsEnrichment instance
   */
  def parse(
    c: Json,
    schemaKey: SchemaKey,
    localMode: Boolean
  ): ValidatedNel[String, IpLookupsEnrichment] =
    isParseable(c, schemaKey)
      .leftMap(e => NonEmptyList.one(e))
      .flatMap { _ =>
        (
          getArgumentFromName(c, "geo").sequence,
          getArgumentFromName(c, "isp").sequence,
          getArgumentFromName(c, "domain").sequence,
          getArgumentFromName(c, "connectionType").sequence
        ).mapN { IpLookupsEnrichment(_, _, _, _, localMode) }.toEither
      }
      .toValidated

  /**
   * Creates the (URI, String) tuple arguments which are the case class parameters
   * @param conf The ip_lookups enrichment JSON
   * @param name The name of the lookup: "geo", "isp", "organization", "domain"
   * @return None if the database isn't being used, Some(Failure) if its URI is invalid,
   * Some(Success) if it is found
   */
  private def getArgumentFromName(
    conf: Json,
    name: String
  ): Option[ValidatedNel[String, (String, URI, String)]] =
    if (conf.hcursor.downField("parameters").downField(name).focus.isDefined) {
      val uri = CirceUtils.extract[String](conf, "parameters", name, "uri")
      val db = CirceUtils.extract[String](conf, "parameters", name, "database")

      // better-monadic-for
      (for {
        uriAndDb <- (uri.toValidatedNel, db.toValidatedNel).mapN { (_, _) }.toEither
        uri <- getDatabaseUri(uriAndDb._1, uriAndDb._2).leftMap(NonEmptyList.one)
      } yield (name, uri, uriAndDb._2)).toValidated.some
    } else None
}

/**
 * Contains enrichments based on IP address.
 * @param uri Full URI to the MaxMind data file
 * @param database Name of the MaxMind database
 * @param geoTuple (Full URI to the geo lookup MaxMind data file, database name)
 * @param ispTuple (Full URI to the ISP lookup MaxMind data file, database name)
 * @param domainTuple (Full URI to the domain lookup MaxMind data file, database name)
 * @param connectionTypeTuple (Full URI to the netspeed lookup MaxMind data file, database name)
 * @param localMode Whether to use the local MaxMind data file. Enabled for tests.
 */
final case class IpLookupsEnrichment(
  geoTuple: Option[(String, URI, String)],
  ispTuple: Option[(String, URI, String)],
  domainTuple: Option[(String, URI, String)],
  connectionTypeTuple: Option[(String, URI, String)],
  localMode: Boolean
) extends Enrichment {
  private type FinalPath = String
  private type DbEntry = Option[(Option[URI], FinalPath)]

  // Construct a Tuple4 of all the IP Lookup databases
  private val dbs: Tuple4[DbEntry, DbEntry, DbEntry, DbEntry] = {
    def db(dbPath: Option[(String, URI, String)]): DbEntry = dbPath.map {
      case (name, uri, file) =>
        if (localMode) {
          (None, getClass.getResource(file).toURI.getPath)
        } else {
          (Some(uri), "./ip_" + name)
        }
    }
    (db(geoTuple), db(ispTuple), db(domainTuple), db(connectionTypeTuple))
  }

  // Collect the cache paths to install
  override val filesToCache: List[(URI, FinalPath)] =
    (dbs._1 ++ dbs._2 ++ dbs._3 ++ dbs._4).collect {
      case (Some(uri), finalPath) => (uri, finalPath)
    }.toList

  // Must be lazy as we don't have the files copied to
  // the Dist Cache on HDFS yet
  private lazy val ipLookups = {
    def path(db: DbEntry): Option[FinalPath] = db.map(_._2)
    IpLookups(
      path(dbs._1),
      path(dbs._2),
      path(dbs._3),
      path(dbs._4),
      memCache = true,
      lruCache = 20000)
  }

  /**
   * Extract the geo-location using the client IP address.
   * If the IPv4 contains a port, it is removed before performing the lookup.
   * @param geo The IpGeo lookup engine we will use to lookup the client's IP address
   * @param ip The client's IP address to use to lookup the client's geo-location
   * @return an IpLookupResult
   */
  def extractIpInformation(ip: String): IpLookupResult = {
    val res = ip match {
      case EnrichmentManager.IPv4Regex(ipv4WithoutPort) => ipLookups.performLookups(ipv4WithoutPort)
      case _ => ipLookups.performLookups(ip)
    }
    IpLookupResult(res)
  }
}

final case class IpLookupResult(
  ipLocation: Option[Either[Throwable, IpLocation]],
  isp: Option[Either[Throwable, String]],
  organization: Option[Either[Throwable, String]],
  domain: Option[Either[Throwable, String]],
  connectionType: Option[Either[Throwable, String]]
)

object IpLookupResult {
  def apply(ilr: IpLookupRes): IpLookupResult = IpLookupResult(
    ilr.ipLocation.map(_.toEither),
    ilr.isp.map(_.toEither),
    ilr.organization.map(_.toEither),
    ilr.domain.map(_.toEither),
    ilr.connectionType.map(_.toEither)
  )
}
