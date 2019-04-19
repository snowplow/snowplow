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

import cats.data.ValidatedNel
import cats.data.Validated
import cats.syntax.either._
import com.snowplowanalytics.iglu.core.{SchemaCriterion, SchemaKey}
import io.circe._

import utils.CirceUtils

import java.net.{Inet4Address, Inet6Address}
import com.google.common.net.{InetAddresses => GuavaInetAddress}
import scala.util.Try

/** Companion object. Lets us create a AnonIpConf from a Json. */
object AnonIpEnrichment extends ParseableEnrichment {
  override val supportedSchema =
    SchemaCriterion("com.snowplowanalytics.snowplow", "anon_ip", "jsonschema", 1, 0)

  /**
   * Creates an AnonIpEnrichment instance from a Json.
   * @param c The anon_ip enrichment JSON
   * @param schemaKey provided for the enrichment, must be supported by this enrichment
   * @return an AnonIpEnrichment configuration
   */
  override def parse(
    config: Json,
    schemaKey: SchemaKey,
    localMode: Boolean = false
  ): ValidatedNel[String, AnonIpConf] =
    (for {
      _ <- isParseable(config, schemaKey)
      paramIPv4Octet <- CirceUtils
        .extract[Int](config, "parameters", "anonOctets")
        .toEither
      paramIPv6Segment <- CirceUtils
        .extract[Int](config, "parameters", "anonSegments")
        .orElse(Validated.valid(paramIPv4Octet))
        .toEither
      ipv4Octets <- AnonIPv4Octets.fromInt(paramIPv4Octet)
      ipv6Segment <- AnonIPv6Segments.fromInt(paramIPv6Segment)
    } yield AnonIpConf(ipv4Octets, ipv6Segment)).toValidatedNel
}

/** How many octets (ipv4) to anonymize */
object AnonIPv4Octets extends Enumeration {
  type AnonIPv4Octets = Value
  val One = Value(1, "1")
  val Two = Value(2, "2")
  val Three = Value(3, "3")
  val All = Value(4, "4")

  /**
   * Convert a Stringly-typed integer into the corresponding AnonOctets Enum Value.
   * Update the Validation Error if the conversion isn't possible.
   * @param anonIPv4Octets A String holding the number of IP address octets to anonymize
   * @return `Right(AnonIPv4Octets)` or `Left(errorMsg)`
   */
  def fromInt(anonIPv4Octets: Int): Either[String, AnonIPv4Octets] =
    Either
      .catchNonFatal(AnonIPv4Octets(anonIPv4Octets))
      .leftMap(
        e =>
          s"IPv4 address octets to anonymize must be 1, 2, 3 or 4. Value: $anonIPv4Octets was given. Error: [${e.getMessage}]"
      )
}

/**
 * How many segments (ipv6) to anonymize?
 */
object AnonIPv6Segments extends Enumeration {

  type AnonIPv6Segments = Value

  val One = Value(1, "1")
  val Two = Value(2, "2")
  val Three = Value(3, "3")
  val Four = Value(4, "4")
  val Five = Value(5, "5")
  val Six = Value(6, "6")
  val Seven = Value(7, "7")
  val All = Value(8, "8")

  /**
   * Convert a Stringly-typed integer
   * into the corresponding AnonIPv6Segments
   * Enum Value.
   *
   * Update the Validation Error if the
   * conversion isn't possible.
   *
   * @param anonIPv6Segments A String holding
   *        the number of IPv6 address
   *        segments to anonymize
   * @return `Right(AnonIPv6Segments)` or `Left(errorMsg)`
   */
  def fromInt(anonIPv6Segments: Int): Either[String, AnonIPv6Segments] =
    Either
      .catchNonFatal(AnonIPv6Segments(anonIPv6Segments))
      .leftMap(
        e =>
          s"IPv6 address segments to anonymize must be 1, 2, 3, 4, 5, 6, 7 or 8. Value $anonIPv6Segments was given. Error: [${e.getMessage}]"
      )
}

/**
 * Instance of AnonIP Enrichment
 *
 * Examples:
 *
 * val enrichment = AnonIpEnrichment(Three, Four)
 * enrichment.anonymizeIp("94.15.223.151") => "94.x.x.x"
 * enrichment.anonymizeIp("2605:2700:0:3::4713:93e3") => "2605:2700:0:3:x:x:x:x"
 *
 * For IPv6 either the form defined in RFC 2732
 *  or the literal IPv6 address format defined in RFC 2373 is accepted
 *
 * @param ipv4Octets The number of octets (IPv4) to anonymize, starting from the right
 * @param ipv6Segments The number of segments (IPv6) to anonymize, starting from the right
 */
final case class AnonIpEnrichment(
  ipv4Octets: AnonIPv4Octets.AnonIPv4Octets,
  ipv6Segments: AnonIPv6Segments.AnonIPv6Segments
) extends Enrichment {

  val IPv4MappedAddressPrefix = "::FFFF:"
  val MaskChar = "x"

  /**
   * Anonymize the supplied IP address.
   *
   * TODO: potentially update this to return
   * a Validation error or a null if the IP
   * address is somehow invalid or incomplete.
   *
   * @param ipOrNull The IP address to anonymize
   * @return the anonymized IP address
   */
  def anonymizeIp(ipOrNull: String): String =
    Option(ipOrNull).map { ip =>
      Try(GuavaInetAddress.forString(ip))
        .map {
          case _: Inet4Address => anonymizeIpV4(ip)
          case ipv6: Inet6Address => anonymizeIpV6(ipv6.getHostAddress)
        }
        .getOrElse(tryAnonymizingInvalidIp(ip))
    }.orNull

  private def anonymizeIpV4(ip: String): String = {
    def mask(ipv4: String) = {
      val split = ipv4.split("\\.")
      split
        .take(AnonIPv4Octets.All.id - ipv4Octets.id)
        .toList
        .padTo(split.size, MaskChar)
        .mkString(".")
    }

    if (ip.startsWith(IPv4MappedAddressPrefix))
      IPv4MappedAddressPrefix + mask(ip.replace(IPv4MappedAddressPrefix, ""))
    else mask(ip)
  }

  private def anonymizeIpV6(ip: String): String =
    ip.split(":")
      .take(AnonIPv6Segments.All.id - ipv6Segments.id)
      .toList
      .padTo(AnonIPv6Segments.All.id, MaskChar)
      .mkString(":")

  /**
   * Mainly to not brake code that already exists, i.e. broken IP like this: "777.2.23"
   * */
  private def tryAnonymizingInvalidIp(ip: String): String =
    if (ip.contains(".") || ip.isEmpty) anonymizeIpV4(ip)
    else if (ip.contains(":")) anonymizeIpV6(ip)
    else ip

}
