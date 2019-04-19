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
  override val supportedSchema =
    SchemaCriterion("com.snowplowanalytics.snowplow", "ip_lookups", "jsonschema", 2, 0)

  /**
   * Creates an IpLookupsConf from a Json.
   * @param c The ip_lookups enrichment JSON
   * @param schemaKey provided for the enrichment, must be supported  by this enrichment
   * @param localMode Whether to use the local MaxMind data file, enabled for tests
   * @return a IpLookups configuration
   */
  override def parse(
    c: Json,
    schemaKey: SchemaKey,
    localMode: Boolean
  ): ValidatedNel[String, IpLookupsConf] =
    isParseable(c, schemaKey)
      .leftMap(e => NonEmptyList.one(e))
      .flatMap { _ =>
        (
          getArgumentFromName(c, "geo").sequence,
          getArgumentFromName(c, "isp").sequence,
          getArgumentFromName(c, "domain").sequence,
          getArgumentFromName(c, "connectionType").sequence
        ).mapN { (geo, isp, domain, connection) =>
          IpLookupsConf(
            file(geo, localMode),
            file(isp, localMode),
            file(domain, localMode),
            file(connection, localMode)
          )
        }.toEither
      }
      .toValidated

  private def file(db: Option[IpLookupsDatabase], localMode: Boolean): Option[(URI, String)] =
    db.map { d =>
      if (localMode) (d.uri, getClass.getResource(d.db).toURI.getPath)
      else (d.uri, s"./ip_${d.name}")
    }

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
  ): Option[ValidatedNel[String, IpLookupsDatabase]] =
    if (conf.hcursor.downField("parameters").downField(name).focus.isDefined) {
      val uri = CirceUtils.extract[String](conf, "parameters", name, "uri")
      val db = CirceUtils.extract[String](conf, "parameters", name, "database")

      // better-monadic-for
      (for {
        uriAndDb <- (uri.toValidatedNel, db.toValidatedNel).mapN { (_, _) }.toEither
        uri <- getDatabaseUri(uriAndDb._1, uriAndDb._2).leftMap(NonEmptyList.one)
      } yield IpLookupsDatabase(name, uri, uriAndDb._2)).toValidated.some
    } else None
}

/**
 * Contains enrichments based on IP address.
 * @param ipLookups IP lookups client
 */
final case class IpLookupsEnrichment(ipLookups: IpLookups) extends Enrichment {

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

private[enrichments] final case class IpLookupsDatabase(
  name: String,
  uri: URI,
  db: String
)
