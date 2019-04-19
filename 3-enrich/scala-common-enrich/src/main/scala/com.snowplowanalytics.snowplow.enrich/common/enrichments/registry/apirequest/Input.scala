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
package enrichments.registry.apirequest

import scala.util.control.NonFatal

import cats.data.ValidatedNel
import cats.implicits._
import cats.kernel.Semigroup
import io.circe._
import io.gatling.jsonpath.{JsonPath => GatlingJsonPath}

import outputs.EnrichedEvent
import utils.JsonPath._

/**
 * Container for key with one (and only one) of possible input sources
 * Basically, represents a key for future template context and way to get value
 * out of EnrichedEvent, custom context, derived event or unstruct event.
 * @param key extracted key
 * @param pojo optional POJO source to take stright from `EnrichedEvent`
 * @param json optional JSON source to take from context or unstruct event
 */
case class Input(
  key: String,
  pojo: Option[PojoInput],
  json: Option[JsonInput]
) {
  import Input._

  // Constructor validation for mapping JSON to `Input` instance
  (pojo, json) match {
    case (None, None) =>
      throw new Exception(
        "API Request Enrichment Input must represent either JSON OR POJO, none present"
      )
    case (Some(_), Some(_)) =>
      throw new Exception(
        "API Request Enrichment Input must represent either JSON OR POJO, both present"
      )
    case _ =>
  }

  // We could short-circuit enrichment process on invalid JSONPath,
  // but it won't give user meaningful error message
  def validatedJsonPath: Either[String, GatlingJsonPath] =
    json.map(_.jsonPath).map(compileQuery) match {
      case Some(compiledQuery) => compiledQuery
      case None => "No JSON Input with JSONPath was given".asLeft
    }

  /**
   * Get key-value pair input from specific `event` for composing
   * @param event currently enriching event
   * @return template context with empty or with single element this particular input
   */
  def getFromEvent(event: EnrichedEvent): TemplateContext = pojo match {
    case Some(pojoInput) => {
      try {
        val method = event.getClass.getMethod(pojoInput.field)
        val value = Option(method.invoke(event)).map(_.toString)
        value.map(v => Map(key -> v)).validNel
      } catch {
        case NonFatal(err) => s"Error accessing POJO input field [$key]: [$err]".invalidNel
      }
    }
    case None => emptyTemplateContext
  }

  /**
   * Get value out of list of JSON contexts
   * @param derived list of self-describing JObjects representing derived contexts
   * @param custom list of self-describing JObjects representing custom contexts
   * @param unstruct optional self-describing JObject representing unstruct event
   * @return template context with empty or with single element this particular input
   */
  def getFromJson(
    derived: List[Json],
    custom: List[Json],
    unstruct: Option[Json]
  ): TemplateContext =
    json match {
      case Some(jsonInput) =>
        val validatedJson = jsonInput.field match {
          case "derived_contexts" =>
            getBySchemaCriterion(derived, jsonInput.schemaCriterion).validNel
          case "contexts" => getBySchemaCriterion(custom, jsonInput.schemaCriterion).validNel
          case "unstruct_event" =>
            getBySchemaCriterion(unstruct.toList, jsonInput.schemaCriterion).validNel
          case other =>
            s"Error: wrong field [$other] passed to Input.getFromJson. Should be one of: derived_contexts, contexts, unstruct_event".invalidNel
        }

        (validatedJson, validatedJsonPath.toValidatedNel).mapN { (validJson, jsonPath) =>
          validJson
            .map(jsonPath.circeQuery) // Query context/UE (always valid)
            .map(wrapArray) // Check if array
            .flatMap(stringifyJson) // Transform to valid string
            .map(v => Map(key -> v)) // Transform to Key-Value
        }
      case None => emptyTemplateContext
    }
}

/**
 * Describes how to take key from POJO source
 * @param field `EnrichedEvent` object field
 */
case class PojoInput(field: String)

/**
 * @param field where to get this JSON, one of unstruct_event, contexts or derived_contexts
 * @param schemaCriterion self-describing JSON you are looking for in the given JSON field.
 * You can specify only the SchemaVer MODEL (e.g. 1-), MODEL plus REVISION (e.g. 1-1-) etc
 * @param jsonPath JSONPath statement to navigate to the field inside the JSON that you want to use
 * as the input
 */
case class JsonInput(
  field: String,
  schemaCriterion: String,
  jsonPath: String
)

/**
 * Companion object, containing common methods for input data manipulation and
 * template context building
 */
object Input {

  /**
   * Validated Optional Map of Strings used to inject values into corresponding placeholders
   * (key inside double curly braces) in template strings
   * Failure means failure while accessing particular field, like invalid JSONPath, POJO-access, etc
   * None means any of required fields were not found, so this lookup need to be skipped in future
   * Tag used to not merge values on colliding keys (`Tags.FirstVal` can be used as well)
   */
  type TemplateContext = ValidatedNel[String, Option[Map[String, String]]]

  val emptyTemplateContext: TemplateContext =
    Map.empty[String, String].some.validNel

  // TODO: use iglu-client 0.4.0
  private val criterionRegex =
    "^(iglu:[a-zA-Z0-9-_.]+/[a-zA-Z0-9-_]+/[a-zA-Z0-9-_]+/)([1-9][0-9]*|\\*)-((?:0|[1-9][0-9]*)|\\*)-((?:0|[1-9][0-9]*)|\\*)$".r

  private final case class M(m: Map[String, String]) extends AnyVal
  private object M {
    implicit def lastValSemigroup: Semigroup[M] = new Semigroup[M] {
      def combine(a: M, b: M): M = M((a.m.toList |+| b.m.toList).toMap)
    }
  }

  /**
   * Get template context out of input configurations
   * If any of inputs missing it will return None
   * @param inputs input-configurations with for keys and instructions how to get values
   * @param event current enriching event
   * @param derivedContexts list of contexts derived on enrichment process
   * @param customContexts list of custom contexts shredded out of event
   * @param unstructEvent optional unstruct event object
   * @return final template context
   */
  def buildTemplateContext(
    inputs: List[Input],
    event: EnrichedEvent,
    derivedContexts: List[Json],
    customContexts: List[Json],
    unstructEvent: Option[Json]
  ): TemplateContext = {
    val eventInputs = buildInputsMap(inputs.map(_.getFromEvent(event)))
    val jsonInputs = buildInputsMap(
      inputs.map(_.getFromJson(derivedContexts, customContexts, unstructEvent))
    )
    (eventInputs.map(_.map(M.apply)) |+| jsonInputs.map(_.map(M.apply))).map(_.map(_.m))
  }

  /**
   * Get data out of all JSON contexts matching `schemaCriterion`
   * If more than one context match schemaCriterion, first will be picked
   * @param contexts list of self-describing JSON contexts attached to event
   * @param schemaCriterion part of URI
   * @return first (optional) self-desc JSON matched `schemaCriterion`
   */
  def getBySchemaCriterion(contexts: List[Json], schemaCriterion: String): Option[Json] =
    criterionMatch(schemaCriterion).flatMap { criterion =>
      val matched = contexts.filter { context =>
        context.hcursor.get[String]("schema").toOption.map(_.startsWith(criterion)).getOrElse(false)
      }
      matched.map(_.hcursor.downField("data").focus).flatten.headOption
    }

  /**
   * Transform Schema Criterion to plain string without asterisks
   * @param schemaCriterion schema criterion of format "iglu:vendor/name/schematype/1-*-*"
   * @return schema criterion of format iglu:vendor/name/schematype/1-
   */
  private def criterionMatch(schemaCriterion: String): Option[String] =
    schemaCriterion match {
      case criterionRegex(schema, "*", _, _) => s"$schema".some
      case criterionRegex(schema, m, "*", _) => s"$schema$m-".some
      case criterionRegex(schema, m, rev, "*") => s"$schema$m-$rev-".some
      case criterionRegex(schema, m, rev, add) => s"$schema$m-$rev-$add".some
      case _ => None
    }

  /**
   * Build and merge template context out of list of all inputs
   * @param kvPairs list of validated optional (empty/single) kv pairs derived from POJO and JSON
   * inputs
   * @return validated optional template context
   */
  def buildInputsMap(kvPairs: List[TemplateContext]): TemplateContext =
    kvPairs.sequence // Swap List[Validation[F, Option[Map[K, V]]]] with Validation[F, List[Option[Map[K, V]]]]
      .map(
        _.sequence // Swap List[Option[Map[K, V]]] with Option[List[Map[K, V]]]
          .map(_.foldLeft(List.empty[(String, String)]) { (acc, e) =>
            acc |+| e.toList
          }.toMap)
      ) // Reduce List[Map[K, V]] to Map[K, V]

  /**
   * Helper function to stringify JValue to URL-friendly format
   * JValue should be converted to string for further use in URL template with following rules:
   * 1. string -> as is
   * 2. number, booleans, nulls -> stringify
   * 3. array -> concatenate with comma ([1,true,"foo"] -> "1,true,foo"). Nested will be flattened
   * 4. object -> use as is
   * @param json arbitrary JSON value
   * @return some string best represenging json or None if there's no way to stringify it
   */
  private def stringifyJson(json: Json): Option[String] =
    json.fold(
      "null".some,
      _.toString.some,
      _.toString.some,
      _.some,
      _.map(stringifyJson).mkString(",").some,
      o => Json.fromJsonObject(o).noSpaces.some
    )
}
