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
package sqlquery

import scala.collection.immutable.IntMap

import cats.syntax.either._
import com.snowplowanalytics.iglu.client.{JsonSchemaPair, SchemaCriterion, SchemaKey}
import com.snowplowanalytics.iglu.client.validation.ProcessingMessageMethods._
import io.circe._
import io.circe.generic.auto._
import io.circe.jackson._
import io.circe.syntax._
import scalaz._
import Scalaz._

import outputs.EnrichedEvent
import utils.ScalazCirceUtils

/** Lets us create an SqlQueryEnrichmentConfig from a Json */
object SqlQueryEnrichmentConfig extends ParseableEnrichment {

  val supportedSchema =
    SchemaCriterion(
      "com.snowplowanalytics.snowplow.enrichments",
      "sql_query_enrichment_config",
      "jsonschema",
      1,
      0,
      0)

  /**
   * Creates an SqlQueryEnrichment instance from a Json.
   * @param config The enrichment JSON
   * @param schemaKey provided for the enrichment, must be supported by this enrichment
   * @return a configured SqlQueryEnrichment instance
   */
  def parse(config: Json, schemaKey: SchemaKey): ValidatedNelMessage[SqlQueryEnrichment] =
    isParseable(config, schemaKey).flatMap(conf => {
      (for {
        // input ctor throws exception
        inputs <- Either.catchNonFatal(
          ScalazCirceUtils.extract[List[Input]](config, "parameters", "inputs")
        ) match {
          case Left(e) => e.getMessage.toProcessingMessage.fail
          case Right(r) => r
        }
        db <- ScalazCirceUtils.extract[Db](config, "parameters", "database")
        query <- ScalazCirceUtils.extract[Query](config, "parameters", "query")
        // output ctor throws exception
        output <- Either.catchNonFatal(
          ScalazCirceUtils.extract[Output](config, "parameters", "output")
        ) match {
          case Left(e) => e.getMessage.toProcessingMessage.fail
          case Right(r) => r
        }
        cache <- ScalazCirceUtils.extract[Cache](config, "parameters", "cache")
      } yield SqlQueryEnrichment(inputs, db, query, output, cache)).toValidationNel
    })
}

case class SqlQueryEnrichment(
  inputs: List[Input],
  db: Db,
  query: Query,
  output: Output,
  cache: Cache
) extends Enrichment {
  import SqlQueryEnrichment._

  /**
   * Primary function of the enrichment. Failure means connection failure, failed unexpected
   * JSON-value, etc. Successful Nil skipped lookup (unfilled placeholder for eg, empty response)
   * @param event currently enriching event
   * @param derivedContexts derived contexts as list of JSON objects
   * @param customContexts custom contexts as [[JsonSchemaPairs]]
   * @param unstructEvent unstructured (self-describing) event as empty or single element
   * [[JsonSchemaPairs]]
   * @return Nil if some inputs were missing, validated JSON contexts if lookup performed
   */
  def lookup(
    event: EnrichedEvent,
    derivedContexts: List[Json],
    customContexts: List[JsonSchemaPair],
    unstructEvent: List[JsonSchemaPair]
  ): ValidationNel[String, List[Json]] = {
    val jsonCustomContexts = transformRawPairs(customContexts)
    val jsonUnstructEvent = transformRawPairs(unstructEvent).headOption

    val placeholderMap: Validated[Input.PlaceholderMap] =
      Input
        .buildPlaceholderMap(inputs, event, derivedContexts, jsonCustomContexts, jsonUnstructEvent)
        .flatMap(allPlaceholdersFilled)
        .leftMap(_.map(_.toString))

    placeholderMap match {
      case Success(Some(intMap)) => get(intMap).leftMap(_.toString).validation.toValidationNel
      case Success(None) => Nil.successNel
      case Failure(err) => err.map(_.toString).failure
    }
  }

  /**
   * Get contexts from cache or perform query if nothing found and put result into cache
   * @param intMap IntMap of extracted values
   * @return validated list of Self-describing contexts
   */
  def get(intMap: IntMap[Input.ExtractedValue]): ThrowableXor[List[Json]] =
    cache.get(intMap) match {
      case Some(response) => response
      case None =>
        val result = query(intMap)
        cache.put(intMap, result)
        result
    }

  /**
   * Perform SQL query and convert result to JSON object
   * @param intMap map with values extracted from inputs and ready to be set placeholders in
   * prepared statement
   * @return validated list of Self-describing contexts
   */
  def query(intMap: IntMap[Input.ExtractedValue]): ThrowableXor[List[Json]] =
    for {
      sqlQuery <- db.createStatement(query.sql, intMap)
      resultSet <- db.execute(sqlQuery)
      context <- output.convert(resultSet)
    } yield context

  /**
   * Transform [[Input.PlaceholderMap]] to None if not enough input values were extracted
   * This prevents db from start building a statement while not failing event enrichment
   * @param placeholderMap some IntMap with extracted values or None if it is known already that not
   * all values were extracted
   * @return Some unchanged value if all placeholder were filled, None otherwise
   */
  private def allPlaceholdersFilled(
    placeholderMap: Input.PlaceholderMap): Validated[Input.PlaceholderMap] =
    getPlaceholderCount.map { placeholderCount =>
      placeholderMap match {
        case Some(intMap) if intMap.keys.size == placeholderCount => Some(intMap)
        case _ => None
      }
    }

  /** Stored amount of ?-signs in query.sql. Initialized once */
  private var lastPlaceholderCount: Validation[Throwable, Int] =
    InvalidStateException("SQL Query Enrichment: placeholderCount hasn't been initialized").failure

  /**
   * If lastPlaceholderCount is successful return it
   * If it's unsucessfult - try to count save result for future use
   */
  def getPlaceholderCount: ValidationNel[String, Int] = lastPlaceholderCount match {
    case Success(count) => count.success
    case Failure(_) =>
      val newCount = db.getPlaceholderCount(query.sql).validation
      lastPlaceholderCount = newCount
      newCount.leftMap(_.toString).toValidationNel
  }
}

/** Companion object containing common methods for requests and manipulating data */
object SqlQueryEnrichment {

  /**
   * Transform pairs of schema and node obtained from [[utils.shredder.Shredder]] into list of
   * regular self-describing JObject representing custom context or unstructured event.
   * If node isn't Self-describing (doesn't contain data key) it will be filtered out.
   * @param pairs list of pairs consisting of schema and Json nodes
   * @return list of regular JObjects
   */
  def transformRawPairs(pairs: List[JsonSchemaPair]): List[Json] =
    pairs.map {
      case (schema, node) =>
        val uri = schema.toSchemaUri
        val data = jacksonToCirce(node)
        data.hcursor.downField("data").focus.map { json =>
          Json.obj(
            "schema" := Json.fromString(uri),
            "data" := json
          )
        }
    }.flatten
}
