/*
 * Copyright (c) 2012-2018 Snowplow Analytics Ltd. All rights reserved.
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
package enrichments.registry.sqlquery

// Scala
import scala.collection.immutable.IntMap
import scala.util.control.NonFatal

// Java
import java.sql.PreparedStatement

// JSONPath
import io.gatling.jsonpath.JsonPath

// Scalaz
import scalaz._
import Scalaz._

// Json4s
import org.json4s._

// This project
import utils.JsonPath._
import outputs.EnrichedEvent

/**
 * Container for key with one (and only one) of possible input sources
 * Basically, represents a key for future template context and way to get value
 * out of [[EnrichedEvent]], custom context, derived event or unstruct event.
 *
 * @param placeholder extracted key
 * @param pojo optional pojo source to take straight from [[EnrichedEvent]]
 * @param json optional JSON source to take from context or unstruct event
 */
case class Input(placeholder: Int, pojo: Option[PojoInput], json: Option[JsonInput]) {
  import Input._

  // Constructor validation for mapping JSON to `Input` instance
  (pojo, json) match {
    case (None, None) =>
      throw new MappingException("Input must represent either json OR pojo, none present")
    case (Some(_), Some(_)) =>
      throw new MappingException("Input must represent either json OR pojo, both present")
    case _ => ()
  }

  // We could short-circuit enrichment process on invalid JSONPath,
  // but it won't give user meaningful error message
  val validatedJsonPath = json.map(_.jsonPath).map(compileQuery) match {
    case Some(compiledQuery) => compiledQuery
    case None                => "No JSON Input with JSONPath was given".failure
  }

  /**
   * Get placeholder/input value pair from specific `event` for composing
   *
   * @param event currently enriching event
   * @return validated pair of placeholder's postition and extracted value ready
   *         to be set on [[PreparedStatement]]
   */
  def getFromEvent(event: EnrichedEvent): ValidationNel[Throwable, (Int, Option[ExtractedValue])] = pojo match {
    case Some(pojoInput) =>
      getFieldType(pojoInput.field) match {
        case Some(placeholderType) =>
          try {
            val anyRef = event.getClass.getMethod(pojoInput.field).invoke(event)
            val option = Option(anyRef.asInstanceOf[placeholderType.PlaceholderType])
            (placeholder, option.map(placeholderType.Value.apply)).successNel
          } catch {
            case NonFatal(e) =>
              InvalidInput("SQL Query Enrichment: Extracting from POJO failed: " + e.toString).failureNel
          }
        case None => InvalidInput("SQL Query Enrichment: Wrong POJO input field was specified").failureNel
      }

    case None => (placeholder, none).successNel
  }

  /**
   * Get placeholder-value pair input from list of JSON contexts
   *
   * @param derived list of self-describing JObjects representing derived contexts
   * @param custom list of self-describing JObjects representing custom contexts
   * @param unstruct optional self-describing JObject representing unstruct event
   * @return validated pair of placeholder's postition and extracted value ready
   *         to be setted on [[PreparedStatement]]
   */
  def getFromJson(derived: List[JObject],
                  custom: List[JObject],
                  unstruct: Option[JObject]): ValidationNel[Throwable, (Int, Option[ExtractedValue])] =
    json match {
      case Some(jsonInput) =>
        jsonInput.extract(derived, custom, unstruct).map(json => (placeholder, json.flatMap(extractFromJson)))
      case None => (placeholder, none).successNel
    }
}

/**
 * Describes how to take key from POJO source
 *
 * @param field `EnrichedEvent` object field
 */
case class PojoInput(field: String)

/**
 *
 * @param field where to get this json, one of unstruct_event, contexts or derived_contexts
 * @param schemaCriterion self-describing JSON you are looking for in the given JSON field.
 *                        You can specify only the SchemaVer MODEL (e.g. 1-), MODEL plus REVISION (e.g. 1-1-) etc
 * @param jsonPath JSON Path statement to navigate to the field inside the JSON that you want to use as the input
 */
case class JsonInput(field: String, schemaCriterion: String, jsonPath: String) {
  import Input._

  /**
   * Extract JSON from contexts or unstruct event
   *
   * @param derived list of derived contexts
   * @param custom list of custom contexts
   * @param unstruct optional unstruct event
   * @return validated optional JSON
   *         failure means fatal error which should abort enrichment
   *         none means not-found value
   */
  def extract(derived: List[JObject],
              custom: List[JObject],
              unstruct: Option[JObject]): ValidationNel[Throwable, Option[JValue]] = {
    val validatedJson = field match {
      case "derived_contexts" => getBySchemaCriterion(derived, schemaCriterion).successNel
      case "contexts"         => getBySchemaCriterion(custom, schemaCriterion).successNel
      case "unstruct_event"   => getBySchemaCriterion(unstruct.toList, schemaCriterion).successNel
      case other =>
        InvalidInput(
          s"SQL Query Enrichment: wrong field [$other] passed to Input.getFromJson. " +
            "Should be one of: derived_contexts, contexts, unstruct_event").failureNel
    }

    val validatedJsonPath: Validation[Throwable, JsonPath] = compileQuery(jsonPath) match {
      case Success(compiledQuery) => compiledQuery.success
      case Failure(error)         => new Exception(error).failure
    }

    (validatedJsonPath.toValidationNel |@| validatedJson) { (jsonPath, validJson) =>
      validJson
        .map(jsonPath.json4sQuery) // Query context/UE (always valid)
        .map(wrapArray) // Check if array
    }
  }
}

/**
 * Companion object, containing common methods for input data manipulation and
 * template context building
 */
object Input {

  private val criterionRegex =
    "^(iglu:[a-zA-Z0-9-_.]+/[a-zA-Z0-9-_]+/[a-zA-Z0-9-_]+/)([1-9][0-9]*|\\*)-((?:0|[1-9][0-9]*)|\\*)-((?:0|[1-9][0-9]*)|\\*)$".r

  /**
   * Map all properties inside [[EnrichedEvent]] to textual representations of their types
   * It is dynamically configured *once*, when job has started
   */
  val eventTypeMap = classOf[EnrichedEvent].getDeclaredFields
    .map(_.toString.split(' ').toList)
    .collect { case List(_, propertyType, name) => (name.split('.').last, propertyType) }
    .toMap

  /**
   * Map all textual representations of types of [[EnrichedEvent]] properties
   * to corresponding [[StatementPlaceholder]]s
   */
  val typeHandlersMap = Map(
    "java.lang.String"  -> StringPlaceholder,
    "java.lang.Integer" -> IntPlaceholder,
    "java.lang.Byte"    -> BytePlaceholder,
    "java.lang.Float"   -> FloatPlaceholder,
    // Just in case
    "String"        -> StringPlaceholder,
    "scala.Int"     -> IntPlaceholder,
    "scala.Double"  -> DoublePlaceholder,
    "scala.Boolean" -> BooleanPlaceholder
  )

  /**
   * Value extracted from POJO or JSON
   * It is wrapped into [[StatementPlaceholder#Value]], because its real type
   * is unknown in compile time and all we need is its method
   * `.set(preparedStatement: PreparedStatement, placeholder: Int): Unit`
   * to fill [[PreparedStatement]]
   */
  type ExtractedValue = StatementPlaceholder#Value

  /**
   * Optional Int-indexed Map of [[ExtractedValue]]s
   * None means some values were not found and SQL Enrichment shouldn't performed
   */
  type PlaceholderMap = Option[IntMap[ExtractedValue]]

  /**
   * Get data out of all JSON contexts matching `schemaCriterion`
   * If more than one context match schemaCriterion, first will be picked
   *
   * @param contexts list of self-describing JSON contexts attached to event
   * @param schemaCriterion part of URI
   * @return first (optional) self-desc JSON matched `schemaCriterion`
   */
  def getBySchemaCriterion(contexts: List[JObject], schemaCriterion: String): Option[JValue] =
    criterionMatch(schemaCriterion).flatMap { criterion =>
      val matched = contexts.filter { context =>
        context.obj.exists {
          case ("schema", JString(schema)) => schema.startsWith(criterion)
          case _                           => false
        }
      }
      matched.map(_ \ "data").headOption
    }

  /**
   * Transform Schema Criterion to plain string without asterisks
   *
   * @param schemaCriterion schema criterion of format "iglu:vendor/name/schematype/1-*-*"
   * @return schema criterion of format iglu:vendor/name/schematype/1-
   */
  private def criterionMatch(schemaCriterion: String): Option[String] =
    schemaCriterion match {
      case criterionRegex(schema, "*", _, _)   => s"$schema".some
      case criterionRegex(schema, m, "*", _)   => s"$schema$m-".some
      case criterionRegex(schema, m, rev, "*") => s"$schema$m-$rev-".some
      case criterionRegex(schema, m, rev, add) => s"$schema$m-$rev-$add".some
      case _                                   => None
    }

  /**
   * Build [[IntMap]] with all sequental input values
   * It returns [[Failure]] if **any** of inputs were extracted with fatal error
   * (not-found is not a fatal error)
   *
   * @param inputs list of all [[Input]] objects
   * @param event POJO of enriched event
   * @param derivedContexts list of derived contexts
   * @param customContexts list of custom contexts
   * @param unstructEvent optional unstructured event
   * @return IntMap if all input values were extracted without error,
   *         non-empty list of errors otherwise
   */
  def buildPlaceholderMap(inputs: List[Input],
                          event: EnrichedEvent,
                          derivedContexts: List[JObject],
                          customContexts: List[JObject],
                          unstructEvent: Option[JObject]): ValidationNel[Throwable, PlaceholderMap] = {

    val eventInputs = inputs.map(_.getFromEvent(event))
    val jsonInputs  = inputs.map(_.getFromJson(derivedContexts, customContexts, unstructEvent))

    val pairs = (eventInputs ++ jsonInputs).sequenceU
      .asInstanceOf[ValidationNel[Throwable, List[(Int, Option[ExtractedValue])]]]
      .map(_.collect { case (position, Some(value)) => (position, value) })

    // Fail if some indexes are missing
    pairs.map(list => IntMap(list: _*)) match {
      case Success(map) if isConsistent(map) => Some(map).successNel
      case Success(map)                      => None.success
      case Failure(err)                      => err.failure
    }
  }

  /**
   * Check if there any gaps in keys of [[IntMap]] (like 1,2,4,5) and keys
   * contain "1", so they fill all placeholders
   *
   * @param intMap Map with Ints as keys
   * @return true if Map contains no gaps and has "1"
   */
  def isConsistent[V](intMap: IntMap[V]): Boolean = {
    val sortedKeys = intMap.keys.toList.sorted
    val (_, result) = sortedKeys.foldLeft((0, true)) {
      case ((prev, accum), cur) =>
        (cur, prev.succ == cur && accum)
    }
    result && sortedKeys.headOption.map(_ == 1).getOrElse(true)
  }

  /**
   * Convert list of inputs to [[IntMap]] with placeholder as a key
   * It will throw away inputs with clasing placeholders (which is actually
   * valid configuration state). Used only to check consistency of placeholders
   */
  def inputsToIntmap(inputs: List[Input]): IntMap[Input] =
    IntMap(inputs.map(i => (i.placeholder, i)): _*)

  /**
   * Extract runtime-typed (wrapped in [[StatementPlaceholder.Value]]) value from JSON
   * Objects, Arrays and nulls are mapped to None
   *
   * @param json JSON, probably extracted by JSONPath
   * @return Some runtime-typed representation of JSON value
   *         or None if it is object, array, null or JNothing
   */
  def extractFromJson(json: JValue): Option[ExtractedValue] = json match {
    case JString(s)                                              => Some(StringPlaceholder.Value(s))
    case JBool(b)                                                => Some(BooleanPlaceholder.Value(b))
    case JInt(int) if int <= Int.MaxValue && int >= Int.MinValue => Some(IntPlaceholder.Value(int.toInt))
    case JInt(long)                                              => Some(LongPlaceholder.Value(long.toLong))
    case JDouble(d)                                              => Some(DoublePlaceholder.Value(d))
    case _                                                       => None // Objects, Arrays and nulls are invalid ("not-found") values
    // In API Request Enrichment null is valid value
  }

  /**
   * Get [[StatementPlaceholder]] for specified field
   * For e.g. "geo_longitude" => [[FloatPlaceholder]]
   *
   * @param field particular property of [[EnrichedEvent]]
   * @return some
   */
  def getFieldType(field: String): Option[StatementPlaceholder] =
    eventTypeMap.get(field).flatMap(typeHandlersMap.get)

  /**
   * This objects hold a value of some extracted from [[Input]] and
   * know how to set this value to [[PreparedStatement]]
   */
  sealed trait StatementPlaceholder {

    /**
     * This type member represents type of placeholder inside [[PreparedStatement]]
     * Known only in runtime
     */
    type PlaceholderType

    /**
     * Closure that accepts [[PreparedStatement]] and returns setter function which
     * accepts value (one of allowed types) and its position in [[PreparedStatement]]
     *
     * @param preparedStatement statement being mutating
     * @return setter function closed on prepared statement
     */
    protected def getSetter(preparedStatement: PreparedStatement): (Int, PlaceholderType) => Unit

    /**
     * Path-dependent class wrapping runtime-typed object
     */
    case class Value(x: PlaceholderType) {
      def set(preparedStatement: PreparedStatement, placeholder: Int): Unit =
        getSetter(preparedStatement)(placeholder, x)
    }
  }

  object IntPlaceholder extends StatementPlaceholder {
    type PlaceholderType = Int
    def getSetter(preparedStatement: PreparedStatement) =
      preparedStatement.setInt
  }

  object StringPlaceholder extends StatementPlaceholder {
    type PlaceholderType = String
    def getSetter(preparedStatement: PreparedStatement) =
      preparedStatement.setString
  }

  object BytePlaceholder extends StatementPlaceholder {
    type PlaceholderType = Byte
    def getSetter(preparedStatement: PreparedStatement) =
      preparedStatement.setByte
  }

  object BooleanPlaceholder extends StatementPlaceholder {
    type PlaceholderType = Boolean
    def getSetter(preparedStatement: PreparedStatement) =
      preparedStatement.setBoolean
  }

  object FloatPlaceholder extends StatementPlaceholder {
    type PlaceholderType = Float
    def getSetter(preparedStatement: PreparedStatement) =
      preparedStatement.setFloat
  }

  object DoublePlaceholder extends StatementPlaceholder {
    type PlaceholderType = Double
    def getSetter(preparedStatement: PreparedStatement) =
      preparedStatement.setDouble
  }

  object LongPlaceholder extends StatementPlaceholder {
    type PlaceholderType = Long
    def getSetter(preparedStatement: PreparedStatement) =
      preparedStatement.setLong
  }
}
