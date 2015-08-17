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

// Apache Commons
import org.apache.commons.codec.digest.DigestUtils

// Scalaz
import scalaz._
import Scalaz._

// json4s
import org.json4s._
import org.json4s.jackson.JsonMethods

// Iglu
import iglu.client.{
  SchemaCriterion,
  SchemaKey
}
import iglu.client.validation.ProcessingMessageMethods._

// This project
import utils.ScalazJson4sUtils

/**
 * Lets us create an EventFingerprintEnrichmentConfig from a JValue.
 */
object EventFingerprintEnrichmentConfig extends ParseableEnrichment {

  val supportedSchema = SchemaCriterion("com.snowplowanalytics.snowplow", "event_fingerprint_config", "jsonschema", 1, 0)

  /**
   * Creates an EventFingerprintEnrichment instance from a JValue.
   *
   * @param config The enrichment JSON
   * @param schemaKey The SchemaKey provided for the enrichment
   *        Must be a supported SchemaKey for this enrichment
   * @return a configured EventFingerprintEnrichment instance
   */
  def parse(config: JValue, schemaKey: SchemaKey): ValidatedNelMessage[EventFingerprintEnrichment] = {
    isParseable(config, schemaKey).flatMap( conf => {
      (for {
        excludedParameters <- ScalazJson4sUtils.extract[List[String]](config, "parameters", "excludeParameters")
        algorithmName      <- ScalazJson4sUtils.extract[String](config, "parameters", "hashAlgorithm")
        algorithm          <- getAlgorithm(algorithmName)
      } yield EventFingerprintEnrichment(algorithm, excludedParameters)).toValidationNel
    })
  }

  /**
   * Look up the fingerprinting algorithm by name
   *
   * @param algorithmName
   * @return A hashing algorithm
   */
  private def getAlgorithm(algorithmName: String): ValidatedMessage[String => String] = algorithmName match {
    case "MD5" => ((s: String) => DigestUtils.md5Hex(s)).success
    case other => s"[$other] is not a supported event fingerprint generation algorithm".toProcessingMessage.fail
  }

}

/**
 * Companion object
 *
 * @param algorithm Hashing algorithm
 * @param excludedParameters List of querystring parameters to exclude from the calculation
 * @return Event fingerprint
 */
case class EventFingerprintEnrichment(algorithm: String => String, excludedParameters: List[String]) extends Enrichment {

  val version = new DefaultArtifactVersion("0.1.0")

  /**
   * Calculate an event fingerprint using all querystring fields except "stm" ("dvce_sent_tstamp")
   *
   * @param parameterMap
   * @return Event fingerprint
   */
  def getEventFingerprint(parameterMap: Map[String, String]): String = {
     val builder = new StringBuilder
     parameterMap.foreach {
      case (key, value) => if (! excludedParameters.contains(key)) {
        builder.append(value)
      }
    }
    algorithm(builder.toString)
  }
}
