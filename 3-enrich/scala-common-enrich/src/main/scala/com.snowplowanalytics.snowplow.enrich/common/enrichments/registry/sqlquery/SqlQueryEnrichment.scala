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

// Maven Artifact
import org.apache.maven.artifact.versioning.DefaultArtifactVersion

// Scala
import scala.collection.immutable.IntMap

// Scalaz
import scalaz._
import Scalaz._

// json4s
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods.fromJsonNode

// Iglu
import com.snowplowanalytics.iglu.client.{SchemaCriterion, SchemaKey}

// This project
import outputs.EnrichedEvent
import utils.ScalazJson4sUtils

/**
 * Lets us create an SqlQueryEnrichmentConfig from a JValue
 */
object SqlQueryEnrichmentConfig extends ParseableEnrichment {

  implicit val formats = DefaultFormats

  val supportedSchema =
    SchemaCriterion("com.snowplowanalytics.snowplow.enrichments", "sql_query_enrichment_config", "jsonschema", 1, 0, 0)

  /**
   * Creates an SqlQueryEnrichment instance from a JValue.
   *
   * @param config The enrichment JSON
   * @param schemaKey The SchemaKey provided for the enrichment
   *        Must be a supported SchemaKey for this enrichment
   * @return a configured SqlQueryEnrichment instance
   */
  def parse(config: JValue, schemaKey: SchemaKey): ValidatedNelMessage[SqlQueryEnrichment] =
    isParseable(config, schemaKey).flatMap(conf => {
      (for {
        inputs <- ScalazJson4sUtils.extract[List[Input]](config, "parameters", "inputs")
        db     <- ScalazJson4sUtils.extract[Db](config, "parameters", "database")
        query  <- ScalazJson4sUtils.extract[Query](config, "parameters", "query")
        output <- ScalazJson4sUtils.extract[Output](config, "parameters", "output")
        cache  <- ScalazJson4sUtils.extract[Cache](config, "parameters", "cache")
      } yield SqlQueryEnrichment(inputs, db, query, output, cache)).toValidationNel
    })
}

case class SqlQueryEnrichment(inputs: List[Input], db: Db, query: Query, output: Output, cache: Cache)
    extends Enrichment {

  import SqlQueryEnrichment._

  /**
   * Primary function of the enrichment
   * Failure means connection failure, failed unexpected JSON-value, etc
   * Successful Nil skipped lookup (unfilled placeholder for eg, empty response)
   *
   * @param event currently enriching event
   * @param derivedContexts derived contexts as list of JSON objects
   * @param customContexts custom contexts as [[JsonSchemaPairs]]
   * @param unstructEvent unstructured (self-describing) event as empty or single element [[JsonSchemaPairs]]
   * @return Nil if some inputs were missing, validated JSON contexts if lookup performed
   */
  def lookup(
    event: EnrichedEvent,
    derivedContexts: List[JObject],
    customContexts: JsonSchemaPairs,
    unstructEvent: JsonSchemaPairs
  ): ValidationNel[String, List[JObject]] = {

    val jsonCustomContexts = transformRawPairs(customContexts)
    val jsonUnstructEvent  = transformRawPairs(unstructEvent).headOption

    val placeholderMap: Validated[Input.PlaceholderMap] =
      Input
        .buildPlaceholderMap(inputs, event, derivedContexts, jsonCustomContexts, jsonUnstructEvent)
        .flatMap(allPlaceholdersFilled)
        .leftMap(_.map(_.toString))

    placeholderMap match {
      case Success(Some(intMap)) => get(intMap).leftMap(_.toString).validation.toValidationNel
      case Success(None)         => Nil.successNel
      case Failure(err)          => err.map(_.toString).failure
    }
  }

  /**
   * Get contexts from [[cache]] or perform query if nothing found
   * and put result into cache
   *
   * @param intMap IntMap of extracted values
   * @return validated list of Self-describing contexts
   */
  def get(intMap: IntMap[Input.ExtractedValue]): ThrowableXor[List[JObject]] =
    cache.get(intMap) match {
      case Some(response) => response
      case None =>
        val result = query(intMap)
        cache.put(intMap, result)
        result
    }

  /**
   * Perform SQL query and convert result to JSON object
   *
   * @param intMap map with values extracted from inputs and ready to
   *               be set placeholders in prepared statement
   * @return validated list of Self-describing contexts
   */
  def query(intMap: IntMap[Input.ExtractedValue]): ThrowableXor[List[JObject]] =
    for {
      sqlQuery  <- db.createStatement(query.sql, intMap)
      resultSet <- db.execute(sqlQuery)
      context   <- output.convert(resultSet)
    } yield context

  /**
   * Transform [[Input.PlaceholderMap]] to None if not enough input values were extracted
   * This prevents [[db]] from start building a statement while not failing event enrichment
   *
   * @param placeholderMap some IntMap with extracted values or None if it is known
   *                       already that not all values were extracted
   * @return Some unchanged value if all placeholder were filled, None otherwise
   */
  private def allPlaceholdersFilled(placeholderMap: Input.PlaceholderMap): Validated[Input.PlaceholderMap] =
    getPlaceholderCount.map { placeholderCount =>
      placeholderMap match {
        case Some(intMap) if intMap.keys.size == placeholderCount => Some(intMap)
        case _                                                    => None
      }
    }

  /**
   * Stored amount of ?-signs in [[query.sql]]
   * Initialized once
   */
  private var lastPlaceholderCount: Validation[Throwable, Int] =
    InvalidStateException("SQL Query Enrichment: placeholderCount hasn't been initialized").failure

  /**
   * If [[lastPlaceholderCount]] is successful return it
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

/**
 * Companion object containing common methods for requests and manipulating data
 */
object SqlQueryEnrichment {

  private implicit val formats = DefaultFormats

  /**
   * Transform pairs of schema and node obtained from [[utils.shredder.Shredder]]
   * into list of regular self-describing [[JObject]] representing custom context
   * or unstructured event.
   * If node isn't Self-describing (doesn't contain data key)
   * it will be filtered out.
   *
   * @param pairs list of pairs consisting of schema and Json nodes
   * @return list of regular JObjects
   */
  def transformRawPairs(pairs: JsonSchemaPairs): List[JObject] =
    pairs.flatMap {
      case (schema, node) =>
        val uri  = schema.toSchemaUri
        val data = fromJsonNode(node)
        data \ "data" match {
          case JNothing => Nil
          case json     => (("schema" -> uri) ~ ("data" -> json): JObject) :: Nil
        }
    }
}
