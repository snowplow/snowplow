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

// Jackson
import com.fasterxml.jackson.databind.{
  ObjectMapper,
  JsonNode
}

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

  private lazy val Mapper = new ObjectMapper

  /**
   * Decodes a URL-encoded String then validates
   * it as correct JSON.
   */
  val extractUrlEncJson: (Int, String, String, String) => Validation[String, String] = (maxLength, enc, field, str) =>
    CU.decodeString(enc, field, str).flatMap(json => validateAndReformatJson(maxLength, field, json))

  /**
   * Decodes a Base64 (URL safe)-encoded String then
   * validates it as correct JSON.
   */
  val extractBase64EncJson: (Int, String, String) => Validation[String, String] = (maxLength, field, str) =>
    CU.decodeBase64Url(field, str).flatMap(json => validateAndReformatJson(maxLength, field, json))

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
  private[utils] def stripInstanceEtc(message: String): String = {
    message
    .replaceAll("@[0-9a-z]+;", "@xxxxxx;")
    .replaceAll("\\t", "    ")
    .replaceAll("\\p{Cntrl}", "") // Any other control character
    .trim
  }

}
