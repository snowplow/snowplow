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
package com.snowplowanalytics.snowplow.enrich.common
package utils

// Java
import java.lang.{Integer => JInteger}
import java.lang.{Double => JDouble}
import java.lang.{Byte => JByte}

// Scalaz
import scalaz._
import Scalaz._

// Argonaut
import argonaut._
import Argonaut._

// This project
import utils.{ConversionUtils => CU}

/**
 * Contains general purpose extractors and other
 * utilities for JSONs.
 */
object JsonUtils {

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
    extractJson(str)
      .bimap(
        e => "Field [%s]: invalid JSON with parsing error: %s".format(field, e),
        f => f.nospaces)
      .flatMap(j => if (j.length > maxLength) {
        "Field [%s]: reformatted JSON length [%s] exceeds maximum allowed length [%s]".format(field, j.length, maxLength).fail
        } else j.success)

  /**
   * Converts a JSON string into a Validation[String, Json]
   *
   * @param json The JSON string to parse
   * @return a Scalaz Validation, wrapping either an error
   *         String or the extracted Json
   */
  // TODO: handle nulls
  def extractJson(json: String): Validation[String, Json] =
    Parse.parse(json).validation

  /**
   * Returns a field which is a) top-level in the JSON and b) a String
   *
   * @param cjson the Argonaut Cursor containing the event
   * @param field the name of the field in the JSON
   */
  def getTopLevelString(cjson: Cursor, field: String): Option[String] =
    (cjson --\ field) flatMap (_.focus.string) map (s => CU.makeTsvSafe(s) )

  /**
   * Returns a field which is a) top-level in the JSON and b) a JsonObject
   *
   * @param cjson the Argonaut Cursor containing the event
   * @param field the name of the field in the JSON
   */
  def getTopLevelObj(cjson: Cursor, field: String): Option[JsonObject] =
    (cjson --\ field) flatMap (_.focus.obj)

  /**
   * Extracts a JSON field as a String. Makes sure to fix
   * tabs, newlines etc as this will be written directly into
   * a TSV. None -> null too for the same reason.
   *
   * @param field The name of the field being processed
   * @param json The Json object hopefully containing a String
   * @return a Scalaz ValidatedString
   */
  val asString: (String, Json) => ValidatedString = (field, json) => {
    json.string match {
      case Some(str) => CU.makeTsvSafe(str).success
      case None => "JSON field [%s]: [%s] is not extractable as a String".format(field, json).fail
    }
  }

  /**
   * Extracts a JSON field as a JByte
   *
   * @param field The name of the field being processed
   * @param json The Json object hopefully containing a Boolean
   * @return a Scalaz Validation[String, JByte]
   */
  val asJByte: (String, Json) => Validation[String, JByte] = (field, json) => {
    json.bool match {
      case Some(bool) => CU.booleanToJByte(bool).success
      case None => "JSON field [%s]: [%s] is not extractable as a JByte".format(field, json).fail
    }
  }

  /**
   * Extracts a JSON field as a JInteger
   *
   * @param field The name of the field being processed
   * @param json The Json object hopefully containing an Integer
   * @return a Scalaz Validation[String, JInteger]
   */
  val asJInteger: (String, Json) => Validation[String, JInteger] = (field, json) => {
    json.number match {
      case Some(num) => (num.toInt: JInteger).success // May silently round or truncate
      case None => "JSON field [%s]: [%s] is not extractable as a JInteger".format(field, json).fail
    }
  }

  /**
   * Extracts a JSON field as a JDouble
   *
   * @param field The name of the field being processed
   * @param json The Json object hopefully containing a Double
   * @return a Scalaz Validation[String, JDouble]
   */
  val asJDouble: (String, Json) => Validation[String, JDouble] = (field, json) => {
    json.number match {
      case Some(num) => (num.toDouble: JDouble).success // May silently round or truncate
      case None => "JSON field [%s]: [%s] is not extractable as a JDouble".format(field, json).fail
    }
  }
}