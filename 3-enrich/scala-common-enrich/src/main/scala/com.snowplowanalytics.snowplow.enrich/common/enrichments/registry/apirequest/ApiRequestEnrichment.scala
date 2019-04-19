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
package apirequest

import java.util.UUID

import cats.data.{NonEmptyList, ValidatedNel}
import cats.implicits._
import com.snowplowanalytics.iglu.core.{SchemaCriterion, SchemaKey, SelfDescribingData}
import io.circe._
import io.circe.generic.auto._
import io.circe.syntax._

import outputs.EnrichedEvent
import utils.CirceUtils

object ApiRequestEnrichment extends ParseableEnrichment {
  override val supportedSchema =
    SchemaCriterion(
      "com.snowplowanalytics.snowplow.enrichments",
      "api_request_enrichment_config",
      "jsonschema",
      1,
      0,
      0
    )

  /**
   * Creates an ApiRequestEnrichment instance from a JValue.
   * @param c The enrichment JSON
   * @param schemaKey The SchemaKey provided for the enrichment
   * Must be a supported SchemaKey for this enrichment
   * @return a configured ApiRequestEnrichment instance
   */
  override def parse(
    c: Json,
    schemaKey: SchemaKey,
    localMode: Boolean = false
  ): ValidatedNel[String, ApiRequestConf] =
    isParseable(c, schemaKey)
      .leftMap(e => NonEmptyList.one(e))
      .flatMap { _ =>
        // input ctor throws exception
        val inputs: ValidatedNel[String, List[Input]] = Either.catchNonFatal {
          CirceUtils.extract[List[Input]](c, "parameters", "inputs").toValidatedNel
        } match {
          case Left(e) => e.getMessage.invalidNel
          case Right(r) => r
        }
        (
          inputs,
          CirceUtils.extract[HttpApi](c, "parameters", "api", "http").toValidatedNel,
          CirceUtils.extract[List[Output]](c, "parameters", "outputs").toValidatedNel,
          CirceUtils.extract[Cache](c, "parameters", "cache").toValidatedNel
        ).mapN { (inputs, api, outputs, cache) =>
          ApiRequestConf(inputs, api, outputs, cache)
        }.toEither
      }
      .toValidated

  /**
   * Transform pairs of schema and node obtained from [[utils.shredder.Shredder]] into list of
   * regular self-describing instance representing custom context or unstruct event
   * @param pairs list of pairs consisting of schema and Json nodes
   * @return list of regular Json
   */
  def transformRawPairs(pairs: List[SelfDescribingData[Json]]): List[Json] =
    pairs.map { p =>
      val uri = p.schema.toSchemaUri
      Json.obj(
        "schema" := Json.fromString(uri),
        "data" := p.data.hcursor.downField("data").focus.getOrElse(p.data)
      )
    }

  /**
   * Creates an UUID based on url and optional body.
   * @param url URL to query
   * @param body optional request body
   * @return UUID that identifies of the request.
   */
  def cacheKey(url: String, body: Option[String]): String = {
    val contentKey = url + body.getOrElse("")
    UUID.nameUUIDFromBytes(contentKey.getBytes).toString
  }
}

final case class ApiRequestEnrichment(
  inputs: List[Input],
  api: HttpApi,
  outputs: List[Output],
  cache: Cache
) extends Enrichment {
  import ApiRequestEnrichment._

  /**
   * Primary function of the enrichment. Failure means HTTP failure, failed unexpected JSON-value,
   * etc. Successful None skipped lookup (missing key for eg.)
   * @param event currently enriching event
   * @param derivedContexts derived contexts
   * @return none if some inputs were missing, validated JSON context if lookup performed
   */
  def lookup(
    event: EnrichedEvent,
    derivedContexts: List[Json],
    customContexts: List[SelfDescribingData[Json]],
    unstructEvent: List[SelfDescribingData[Json]]
  ): ValidatedNel[String, List[Json]] = {
    // Note that [[JsonSchemaPairs]] have specific structure - it is a pair,
    // where first element is [[SchemaKey]], second element is JSON Object
    // with keys: `data`, `schema` and `hierarchy` and `schema` contains again [[SchemaKey]]
    // but as nested JSON object. `schema` and `hierarchy` can be ignored here
    val jsonCustomContexts = transformRawPairs(customContexts)
    val jsonUnstructEvent = transformRawPairs(unstructEvent).headOption

    val templateContext =
      Input.buildTemplateContext(
        inputs,
        event,
        derivedContexts,
        jsonCustomContexts,
        jsonUnstructEvent
      )

    (for {
      context <- templateContext.toEither
      outputs <- getOutputs(context).leftMap(e => NonEmptyList.one(e))
    } yield outputs).toValidated
  }

  /**
   * Build URI and try to get value for each of [[outputs]]
   * @param validInputs map to build template context
   * @return validated list of lookups, whole lookup will be failed if any of outputs were failed
   */
  private[apirequest] def getOutputs(
    validInputs: Option[Map[String, String]]
  ): Either[String, List[Json]] = {
    val result = for {
      templateContext <- validInputs.toList
      url <- api.buildUrl(templateContext).toList
      output <- outputs
      body = api.buildBody(templateContext)
    } yield cachedOrRequest(url, body, output).leftMap(_.toString)
    result.sequence
  }

  /**
   * Check cache for URL and perform HTTP request if value wasn't found
   * @param url URL to request
   * @param output currently processing output
   * @return validated JObject, in case of success ready to be attached to derived contexts
   */
  private[apirequest] def cachedOrRequest(
    url: String,
    body: Option[String],
    output: Output
  ): Either[Throwable, Json] = {
    val key = cacheKey(url, body)
    val value = cache.get(key) match {
      case Some(cachedResponse) => cachedResponse
      case None =>
        val json = api.perform(url, body).flatMap(output.parseResponse)
        cache.put(key, json)
        json
    }
    value.flatMap(output.extract).map(output.describeJson)
  }
}
