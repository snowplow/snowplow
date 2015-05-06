/*
* Copyright (c) 2014 Snowplow Analytics Ltd. All rights reserved.
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
package utils

// Java
import java.math.{BigInteger => JBigInteger}
import java.net.URLEncoder

// Jackson
import com.fasterxml.jackson.databind.{
  ObjectMapper,
  JsonNode
}

// Joda-Time
import org.joda.time.{DateTime, DateTimeZone}
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}

// Scalaz
import scalaz._
import Scalaz._

// json4s
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

// This project
import utils.{ConversionUtils => CU}

/**
 * Contains general purpose extractors and other
 * utilities for JSONs. Jackson-based.
 */
object JsonUtils {

  type DateTimeFields = Option[Tuple2[NonEmptyList[String], DateTimeFormatter]]

  private lazy val Mapper = new ObjectMapper

  // Defines the maximalist JSON Schema-compatible date-time format
  private val JsonSchemaDateTimeFormat = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").withZone(DateTimeZone.UTC)

  /**
   * Validates a String as correct JSON.
   */
  val extractJson: (Int, String, String) => Validation[String, String] = (maxLength, field, str) =>
    validateAndReformatJson(maxLength, field, str)

  /**
   * Decodes a Base64 (URL safe)-encoded String then
   * validates it as correct JSON.
   */
  val extractBase64EncJson: (Int, String, String) => Validation[String, String] = (maxLength, field, str) =>
    CU.decodeBase64Url(field, str).flatMap(json => validateAndReformatJson(maxLength, field, json))

  /**
   * Converts a Joda DateTime into
   * a JSON Schema-compatible date-time string.
   *
   * @param datetime The Joda DateTime
   *        to convert to a timestamp String
   * @return the timestamp String
   */
  private[utils] def toJsonSchemaDateTime(dateTime: DateTime): String = JsonSchemaDateTimeFormat.print(dateTime)

  /**
   * Converts a boolean-like String of value "true"
   * or "false" to a JBool value of true or false
   * respectively. Any other value becomes a
   * JString.
   *
   * No erroring if the String is not boolean-like,
   * leave it to eventual JSON Schema validation
   * to enforce that.
   *
   * @param str The boolean-like String to convert
   * @return true for "true", false for "false",
   *         and otherwise a JString wrapping the
   *         original String
   */
  private[utils] def booleanToJValue(str: String): JValue = str match {
    case "true" => JBool(true)
    case "false" => JBool(false)
    case _ => JString(str)
  }

  /**
   * Converts an integer-like String to a
   * JInt value. Any other value becomes a
   * JString.
   *
   * No erroring if the String is not integer-like,
   * leave it to eventual JSON Schema validation
   * to enforce that.
   *
   * @param str The integer-like String to convert
   * @return a JInt if the String was integer-like,
   *         or else a JString wrapping the original
   *         String.
   */
  private[utils] def integerToJValue(str: String): JValue =
    try {
      JInt(new JBigInteger(str))
    } catch {
      case nfe: NumberFormatException =>
        JString(str)
    }

  /**
   * Reformats a non-standard date-time into a format
   * compatible with JSON Schema's date-time format
   * validation. If the String does not match the
   * expected date format, then return the original String.
   *
   * @param str The date-time-like String to reformat
   *        to pass JSON Schema validation
   * @return the reformatted date-time String if
   *         possible, or otherwise the original String
   */
  def toJsonSchemaDateTime(str: String, fromFormat: DateTimeFormatter): String =
    try {
      val dt = DateTime.parse(str, fromFormat)
      toJsonSchemaDateTime(dt)
    } catch {
      case iae: IllegalArgumentException => str
    }

  /**
   * Converts an incoming key, value into a json4s JValue.
   * Uses the lists of keys which should contain bools,
   * ints and dates to apply specific processing to
   * those values when found.
   *
   * @param key The key of the field to generate. Also used
   *        to determine what additional processing should
   *        be applied to the value
   * @param value The value of the field
   * @param bools A List of keys whose values should be
   *        processed as boolean-like Strings
   * @param ints A List of keys whose values should be
   *        processed as integer-like Strings
   * @param dates If Some, a NEL of keys whose values should
   *        be treated as date-time-like Strings, which will
   *        require processing from the specified format
   * @return a JField, containing the original key and the
   *         processed String, now as a JValue
   */
  def toJField(key: String, value: String, bools: List[String], ints: List[String],
    dateTimes: DateTimeFields): JField = {

    val v = (value, dateTimes) match {
      case ("", _)                  => JNull
      case _ if bools.contains(key) => booleanToJValue(value)
      case _ if ints.contains(key)  => integerToJValue(value)
      case (_, Some((nel, fmt)))
        if nel.toList.contains(key) => JString(toJsonSchemaDateTime(value, fmt))
      case _                        => JString(value)
    }
    (key, v)
  }

  /**
   * Validates and reformats a JSON:
   * 1. Checks the JSON is valid
   * 2. Reformats, including removing unnecessary whitespace
   * 3. Checks if reformatted JSON is <= maxLength, because
   *    a truncated JSON causes chaos in Redshift et al
   *
   * @param field the name of the field containing the JSON
   * @param str the String hopefully containing JSON
   * @param maxLength the maximum allowed length for this
   *        JSON when reformatted
   * @return a Scalaz Validation, wrapping either an error
   *         String or the reformatted JSON String
   */
  private[utils] def validateAndReformatJson(maxLength: Int, field: String, str: String): Validation[String, String] =
    extractJson(field, str)
      .map(j => compact(fromJsonNode(j)))
      .flatMap(j => if (j.length > maxLength) {
        "Field [%s]: reformatted JSON length [%s] exceeds maximum allowed length [%s]".format(field, j.length, maxLength).fail
        } else j.success)

  /**
   * Converts a JSON string into a Validation[String, JsonNode]
   *
   * @param field The name of the field containing JSON
   * @param instance The JSON string to parse
   * @return a Scalaz Validation, wrapping either an error
   *         String or the extracted JsonNode
   */
  def extractJson(field: String, instance: String): Validation[String, JsonNode] =
    try {
      Mapper.readTree(instance).success
    } catch {
      case e: Throwable => s"Field [$field]: invalid JSON [$instance] with parsing error: ${stripInstanceEtc(e.getMessage)}".fail
    }

  /**
   * Converts a JSON string into a JsonNode.
   *
   * UNSAFE - only use it for Strings you have
   * created yourself. Use extractJson for all
   * external Strings.
   *
   * @param instance The JSON string to parse
   * @return the extracted JsonNode
   */
  def unsafeExtractJson(instance: String): JsonNode =
    Mapper.readTree(instance)

  /**
   * Strips the instance information from a Jackson
   * parsing exception message:
   *
   * "... at [Source: java.io.StringReader@1fe7a8f8; line: 1, column: 2]""
   *                                       ^^^^^^^^
   *
   * Also removes any control characters and replaces
   * tabs with 4 spaces.
   *
   * @param message The exception message which needs
   *        tidying up
   * @return the same exception message, but with
   *         instance information etc removed
   */
  def stripInstanceEtc(message: String): String = {
    message
    .replaceAll("@[0-9a-z]+;", "@xxxxxx;")
    .replaceAll("\\t", "    ")
    .replaceAll("\\p{Cntrl}", "") // Any other control character
    .trim
  }
}
