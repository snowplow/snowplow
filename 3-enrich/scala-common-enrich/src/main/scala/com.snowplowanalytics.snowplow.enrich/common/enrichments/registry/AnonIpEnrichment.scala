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
import cats.syntax.either._
import com.snowplowanalytics.iglu.core.{SchemaCriterion, SchemaKey}
import io.circe._

import utils.CirceUtils

/** Companion object. Lets us create a AnonIpEnrichment from a Json. */
object AnonIpEnrichment extends ParseableEnrichment {

  val supportedSchema =
    SchemaCriterion("com.snowplowanalytics.snowplow", "anon_ip", "jsonschema", 1, 0)

  /**
   * Creates an AnonIpEnrichment instance from a JValue.
   * @param c The anon_ip enrichment JSON
   * @param schemaKey provided for the enrichment, must be supported by this enrichment
   * @return a configured AnonIpEnrichment instance
   */
  def parse(config: Json, schemaKey: SchemaKey): ValidatedNel[String, AnonIpEnrichment] =
    (for {
      _ <- isParseable(config, schemaKey)
      param <- CirceUtils.extract[Int](config, "parameters", "anonOctets").toEither
      octets <- AnonOctets.fromInt(param)
    } yield AnonIpEnrichment(octets)).toValidatedNel
}

/** How many octets to anonymize? */
object AnonOctets extends Enumeration {
  type AnonOctets = Value
  val One = Value(1, "1")
  val Two = Value(2, "2")
  val Three = Value(3, "3")
  val All = Value(4, "4")

  /**
   * Convert a Stringly-typed integer into the corresponding AnonOctets Enum Value.
   * Update the Validation Error if the conversion isn't possible.
   * @param anonOctets A String holding the number of IP address octets to anonymize
   * @return a Validation-boxed AnonOctets
   */
  def fromInt(anonOctets: Int): Either[String, AnonOctets] =
    Either
      .catchNonFatal(AnonOctets(anonOctets))
      .leftMap(e => s"IP address octets to anonymize must be 1, 2, 3 or 4: ${e.getMessage}")
}

/**
 * Config for an anon_ip enrichment
 * @param octets The number of octets to anonymize
 */
case class AnonIpEnrichment(
  octets: AnonOctets.AnonOctets
) extends Enrichment {

  /**
   * Anonymize the supplied IP address. octets is the number of octets in the IP address to
   * anonymize, starting from the right. For example:
   *
   * anonymizeIp("94.15.223.151", One)
   * => "94.15.223.x"
   *
   * anonymizeIp("94.15.223.151", Three)
   * => "94.x.x.x"
   *
   * TODO: potentially update this to return a Validation error or a null if the IP address is
   * somehow invalid or incomplete.
   * @param ip The IP address to anonymize
   * @return the anonymized IP address
   */
  import AnonOctets._
  def anonymizeIp(ip: String): String =
    Option(ip)
      .map(
        _.split("\\.").zipWithIndex
          .map {
            case (q, i) =>
              if (octets.id >= All.id - i) "x" else q
          }
          .mkString(".")
      )
      .orNull
}
