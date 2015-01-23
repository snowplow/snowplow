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

// This project
import utils.ScalazJson4sUtils

/**
* Companion object. Lets us create a AnonIpEnrichment
* from a JValue.
*/
object AnonIpEnrichment extends ParseableEnrichment {

  val supportedSchema = SchemaCriterion("com.snowplowanalytics.snowplow", "anon_ip", "jsonschema", 1, 0)

  /**
   * Creates an AnonIpEnrichment instance from a JValue.
   * 
   * @param config The anon_ip enrichment JSON
   * @param schemaKey The SchemaKey provided for the enrichment
   *        Must be a supported SchemaKey for this enrichment   
   * @return a configured AnonIpEnrichment instance
   */
  def parse(config: JValue, schemaKey: SchemaKey): ValidatedNelMessage[AnonIpEnrichment] = {
    isParseable(config, schemaKey).flatMap( conf => {
      (for {
        param  <- ScalazJson4sUtils.extract[Int](config, "parameters", "anonOctets")
        octets <- AnonOctets.fromInt(param)
        enrich =  AnonIpEnrichment(octets)
      } yield enrich).toValidationNel
    })
  }

}

/**
 * How many octets to anonymize?
 */
object AnonOctets extends Enumeration {

  type AnonOctets = Value
  
  val One   = Value(1, "1")
  val Two   = Value(2, "2")
  val Three = Value(3, "3")
  val All   = Value(4, "4")

  /**
   * Convert a Stringly-typed integer
   * into the corresponding AnonOctets
   * Enum Value.
   *
   * Update the Validation Error if the
   * conversion isn't possible.
   *
   * @param anonOctets A String holding
   *        the number of IP address
   *        octets to anonymize
   * @return a Validation-boxed AnonOctets
   */
  def fromInt(anonOctets: Int): ValidatedMessage[AnonOctets] = {
    try {
      AnonOctets(anonOctets).success
    } catch {
      case nse: NoSuchElementException => "IP address octets to anonymize must be 1, 2, 3 or 4".toProcessingMessage.fail
    }
  }
}

/**
 * Config for an anon_ip enrichment
 *
 * @param octets The number of octets to anonymize
 */
case class AnonIpEnrichment(
  octets: AnonOctets.AnonOctets
  ) extends Enrichment {

  val version = new DefaultArtifactVersion("0.1.0")

  /**
   * Anonymize the supplied IP address.
   *
   * octets is the number of octets
   * in the IP address to anonymize, starting
   * from the right. For example:
   *
   * anonymizeIp("94.15.223.151", One)
   * => "94.15.223.x"
   *
   * anonymizeIp("94.15.223.151", Three)
   * => "94.x.x.x"
   *
   * TODO: potentially update this to return
   * a Validation error or a null if the IP
   * address is somehow invalid or incomplete.
   *
   * @param ip The IP address to anonymize
   * @return the anonymized IP address
   */
  import AnonOctets._
  def anonymizeIp(ip: String): String =
    Option(ip).map(_.split("\\.").zipWithIndex.map{
      case (q, i) => {
        if (octets.id >= All.id - i) "x" else q
      }
    }.mkString(".")).orNull
}
