/*
 * Copyright (c) 2012-2016 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.enrich
package common
package enrichments
package registry
package apirequest

// Maven Artifact
import org.apache.maven.artifact.versioning.DefaultArtifactVersion

// Scalaz
import scalaz._
import Scalaz._

// json4s
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods.fromJsonNode

// Iglu
import com.snowplowanalytics.iglu.client.{ SchemaKey, SchemaCriterion }

// This project
import outputs.EnrichedEvent
import utils.{ HttpClient, ScalazJson4sUtils }

/**
 * Lets us create an ApiRequestEnrichmentConfig from a JValue
 */
object ApiRequestEnrichmentConfig extends ParseableEnrichment {

  val supportedSchema = SchemaCriterion("com.snowplowanalytics.snowplow.enrichments", "api_request_enrichment_config", "jsonschema", 1, 0, 0)

  /**
   * Creates an ApiRequestEnrichment instance from a JValue.
   *
   * @param config The enrichment JSON
   * @param schemaKey The SchemaKey provided for the enrichment
   *        Must be a supported SchemaKey for this enrichment
   * @return a configured ApiRequestEnrichment instance
   */
  def parse(config: JValue, schemaKey: SchemaKey): ValidatedNelMessage[ApiRequestEnrichment] = {
    isParseable(config, schemaKey).flatMap( conf => {
      (for {
        inputs  <- ScalazJson4sUtils.extract[List[Input]](config, "parameters", "inputs")
        httpApi <- ScalazJson4sUtils.extract[HttpApi](config, "parameters", "api", "http")
        outputs <- ScalazJson4sUtils.extract[List[Output]](config, "parameters", "outputs")
        cache   <- ScalazJson4sUtils.extract[Cache](config, "parameters", "cache")
      } yield ApiRequestEnrichment(inputs, httpApi, outputs, cache)).toValidationNel
    })
  }
}

case class ApiRequestEnrichment(inputs: List[Input], api: HttpApi, outputs: List[Output], cache: Cache) extends Enrichment {

  import ApiRequestEnrichment._

  val version = new DefaultArtifactVersion("0.1.0")

  /**
   * Primary function of the enrichment
   * Failure means HTTP failure, failed unexpected JSON-value, etc
   * Successful None skipped lookup (missing key for eg.)
   *
   * @param event currently enriching event
   * @param derivedContexts derived contexts
   * @return none if some inputs were missing, validated JSON context if lookup performed
   */
  def lookup(
      event: EnrichedEvent,
      derivedContexts: List[JObject],
      customContexts: JsonSchemaPairs,
      unstructEvent: JsonSchemaPairs): ValidationNel[String, List[JObject]] = {
    /**
     * Note that [[JsonSchemaPairs]] have specific structure - it is a pair,
     * where first element is [[SchemaKey]], second element is JSON Object
     * with keys: `data`, `schema` and `hierarchy` and `schema` contains again [[SchemaKey]]
     * but as nested JSON object. `schema` and `hierarchy` can be ignored here
     */
    val jsonCustomContexts = transformRawPairs(customContexts)
    val jsonUnstructEvent = transformRawPairs(unstructEvent).headOption

    val templateContext = Input.buildTemplateContext(
      inputs, event, derivedContexts, jsonCustomContexts, jsonUnstructEvent)

    templateContext.flatMap(getOutputs(_).toValidationNel)
  }

  /**
   * Build URI and try to get value for each of [[outputs]]
   *
   * @param validInputs map to build template context
   * @return validated list of lookups, whole lookup will be failed if any of
   *         outputs were failed
   */
  private[apirequest] def getOutputs(validInputs: Option[Map[String, String]]): Validation[String, List[JObject]] = {
    val result = for {
      templateContext <- validInputs.toList
      url <- api.buildUrl(templateContext).toList
      output <- outputs
    } yield cachedOrRequest(url, output).leftMap(_.toString)
    result.sequenceU
  }

  /**
   * Check cache for URL and perform HTTP request if value wasn't found
   *
   * @param url URL to request
   * @param output currently processing output
   * @return validated JObject, in case of success ready to be attached to derived contexts
   */
  private[apirequest] def cachedOrRequest(url: String, output: Output): Validation[Throwable, JObject] = {
    val value = cache.get(url) match {
      case Some(cachedResponse) => cachedResponse
      case None => {
        val json = api.perform(url).flatMap(output.parse)
        cache.put(url, json)
        json
      }
    }
    value.flatMap(output.extract).map(output.describeJson)
  }
}

/**
 * Companion object containing common methods for requests and manipulating data
 */
object ApiRequestEnrichment {
  /**
   * Transform pairs of schema and node obtained from [[utils.shredder.Shredder]]
   * into list of regular self-describing instance representing custom context
   * or unstruct event
   *
   * @param pairs list of pairs consisting of schema and Json nodes
   * @return list of regular JObjects
   */
  def transformRawPairs(pairs: JsonSchemaPairs): List[JObject] =
    pairs.map { case (schema, node) =>
      val uri = schema.toSchemaUri
      val data = fromJsonNode(node)
      ("schema" -> uri) ~ ("data" -> data \ "data")
    }
}
