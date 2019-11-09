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
package com.snowplowanalytics.snowplow.enrich.common.enrichments.registry.sqlquery

import cats.syntax.either._

import io.circe.{Decoder, DecodingFailure, Json}
import io.circe.generic.semiauto._
import io.circe.parser.parse

import com.snowplowanalytics.iglu.core.SchemaKey
import com.snowplowanalytics.iglu.core.circe.CirceIgluCodecs._

import org.joda.time.DateTime

/**
 * Handles JSON-specific output (actually, nothing here is JSON-specific, unlike API Request
 * Enrichment, so all these properties can go into primary
 * Output class as they can be used for *any* output)
 */
final case class JsonOutput(
  schema: SchemaKey,
  describes: Output.DescribeMode,
  propertyNames: JsonOutput.PropertyNameMode
)

object JsonOutput {

  /** ADT specifying how to transform key names */
  sealed trait PropertyNameMode {
    def transform(key: String): String
  }

  /** Some_Column to Some_Column */
  case object AsIs extends PropertyNameMode {
    def transform(key: String): String = key
  }

  /** some_column to someColumn */
  case object CamelCase extends PropertyNameMode {
    def transform(key: String): String =
      "_([a-z\\d])".r.replaceAllIn(key, _.group(1).toUpperCase)
  }

  /** some_column to SomeColumn */
  case object PascalCase extends PropertyNameMode {
    def transform(key: String): String =
      "_([a-z\\d])".r.replaceAllIn(key, _.group(1).toUpperCase).capitalize
  }

  /** SomeColumn to some_column */
  case object SnakeCase extends PropertyNameMode {
    def transform(key: String): String =
      "[A-Z\\d]".r.replaceAllIn(key, "_" + _.group(0).toLowerCase())
  }

  /** SomeColumn to somecolumn */
  case object LowerCase extends PropertyNameMode {
    def transform(key: String): String = key.toLowerCase
  }

  /** SomeColumn to SOMECOLUMN */
  case object UpperCase extends PropertyNameMode {
    def transform(key: String): String = key.toUpperCase
  }

  /** Map of datatypes to JSON-generator functions */
  val resultsetGetters: Map[String, Object => Json] = Map(
    "java.lang.Integer" -> ((obj: Object) => Json.fromInt(obj.asInstanceOf[Int])),
    "java.lang.Long" -> ((obj: Object) => Json.fromLong(obj.asInstanceOf[Long])),
    "java.lang.Boolean" -> ((obj: Object) => Json.fromBoolean(obj.asInstanceOf[Boolean])),
    "java.lang.Double" -> ((obj: Object) => Json.fromDoubleOrNull(obj.asInstanceOf[Double])),
    "java.lang.Float" -> ((obj: Object) => Json.fromDoubleOrNull(obj.asInstanceOf[Float].toDouble)),
    "java.lang.String" -> ((obj: Object) => Json.fromString(obj.asInstanceOf[String])),
    "java.sql.Date" -> (
      (obj: Object) => Json.fromString(new DateTime(obj.asInstanceOf[java.sql.Date]).toString)
    )
  )

  /**
   * Transform value from AnyRef using stringified type hint
   * @param anyRef AnyRef extracted from ResultSet
   * @param datatype stringified type representing AnyRef's real type
   * @return AnyRef converted to JSON
   */
  def getValue(anyRef: AnyRef, datatype: String): Json =
    if (anyRef == null) Json.Null
    else {
      val converter = resultsetGetters.getOrElse(datatype, parseObject)
      converter(anyRef)
    }

  /**
   * Default method to parse unknown column type. First try to parse as JSON Object (PostgreSQL
   * JSON doesn't have a loader for JSON) if not successful parse as JSON String.
   * This method has significant disadvantage, since it can parse string "12 books" as JInt(12),
   * but I don't know better way to handle PostgreSQL JSON.
   */
  val parseObject: Object => Json = obj => {
    val string = obj.toString
    parse(string) match {
      case Right(js) => js
      case _ => Json.fromString(string)
    }
  }

  implicit val jsonOutputCirceDecoder: Decoder[JsonOutput] =
    deriveDecoder[JsonOutput]

  implicit val propertyNameCirceDecoder: Decoder[PropertyNameMode] =
    Decoder.instance { cur =>
      cur.as[String].flatMap { str =>
        str match {
          case "AS_IS" => AsIs.asRight
          case "CAMEL_CASE" => CamelCase.asRight
          case "PASCAL_CASE" => PascalCase.asRight
          case "SNAKE_CASE" => SnakeCase.asRight
          case "LOWER_CASE" => LowerCase.asRight
          case "UPPER_CASE" => UpperCase.asRight
          case _ => DecodingFailure(s"$str is not valid PropertyName", cur.history).asLeft
        }
      }
    }
}
