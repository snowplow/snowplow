/*
 * Copyright (c) 2012-2013 SnowPlow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.enrich.hadoop
package utils

// Java
import java.net.URLDecoder
import java.lang.{Integer => JInteger}
import java.lang.{Float => JFloat}

// Scalaz
import scalaz._
import Scalaz._

/**
 * General-purpose utils to help the
 * ETL process along.
 */
object ConversionUtils {

  /**
   * Decodes a String in the specific encoding,
   * also removing:
   * * Newlines - because they will break Hive
   * * Tabs - because they will break non-Hive
   *          targets (e.g. Infobright)
   *
   * IMPLDIFF: note that this version, unlike
   * the Hive serde version, does not call
   * cleanUri. This is because we cannot assume
   * that str is a URI which needs 'cleaning'.
   *
   * TODO: simplify this when we move to a more
   * robust output format (e.g. Avro) - as then
   * no need to remove line breaks, tabs etc
   *
   * @param enc The encoding of the String
   * @param field The name of the field 
   * @param str The String to decode
   *
   * @return a Scalaz Validation, wrapping either
   *         an error String or the decoded String
   */
  val decodeString: (String, String, String) => Validation[String, String] = (enc, field, str) =>
    try {
      if (str == "null") { // Yech, to handle a bug in the JavaScript tracker
        null.asInstanceOf[String].success
      } else {
        val s = Option(str).getOrElse("")
        val d = URLDecoder.decode(s, enc)
        val r = d.replaceAll("(\\r|\\n)", "")
                 .replaceAll("\\t", "    ")
        r.success
      }
    } catch {
      case e =>
        "Exception decoding [%s] from [%s] encoding: [%s]".format(str, enc, e.getMessage).fail
    }

  /**
   * Extract a Scala Int from
   * a String, or error.
   *
   * @param str The String
   *        which we hope is an
   *        Int
   * @param field The name of the
   *        field we are trying to
   *        process. To use in our
   *        error message
   * @return a Scalaz Validation,
   *         being either a
   *         Failure String or
   *         a Success JInt
   */
  val stringToJInteger: (String, String) => Validation[String, JInteger] = (field, str) =>
    try {
      val jint: JInteger = str.toInt
      jint.success
    } catch {
      case nfe: NumberFormatException =>
        "Field [%s]: cannot convert [%s] to Int".format(field, str).fail
    }

  /**
   * Extract a Scala Float from
   * a String, or error.
   *
   * @param str The String which we hope can
   *        be turned into a Float
   * @param field The name of the field
   *        we are trying to process. To use
   *        in our error message
   * @return a Scalaz Validation, being either
   *         a Failure String or a Success Int
   */
  val stringToJFloat: (String, String) => Validation[String, JFloat] = (field, str) =>
    try {
      if (str == "null") { // Yech, to handle a bug in the JavaScript tracker
        null.asInstanceOf[JFloat].success
      } else {
        val jfloat: JFloat = str.toFloat
        jfloat.success
      }
    } catch {
      case nfe: NumberFormatException =>
        "Field [%s]: cannot convert [%s] to Float".format(field, str).fail
    }

  /**
   * Extract a Scala Byte from
   * a String, or error.
   *
   * @param str The String
   *        which we hope is an
   *        Byte
   * @param field The name of the
   *        field we are trying to
   *        process. To use in our
   *        error message
   * @return a Scalaz Validation,
   *         being either a
   *         Failure String or
   *         a Success Byte
   */
  val stringToByte: (String, String) => Validation[String, Byte] = (field, str) =>
    try {
      str.toByte.success
    } catch {
      case nfe: NumberFormatException =>
        "Field [%s]: cannot convert [%s] to Byte".format(field, str).fail
    }

  /**
   * Converts a String of value "1" or "0"
   * to true or false respectively.
   *
   * @param str The String to convert
   * @return True for "1", false for "0", or
   *         an error message for any other
   *         value, all boxed in a Scalaz
   *         Validation
   */
  def stringToBoolean(str: String): Validation[String, Boolean] = 
    if (str == "1") {
      true.success
    } else if (str == "0") {
      false.success
    } else {
      "Cannot convert [%s] to boolean, only 1 or 0.".format(str).fail
    }

  /**
   * Truncates a String - useful for making sure
   * Strings can't overflow a database field.
   *
   * @param str The String to truncate
   * @param length The maximum length of the String
   *        to keep
   * @return the truncated String
   */
  def truncate(str: String, length: Int): String =
    if (str == null) {
      null
    } else {
      str.take(length)
    }

  /**
   * Helper to convert a Boolean value to a Byte.
   * Does not require any validation.
   *
   * @param bool The Boolean to convert into a Byte
   * @return 0 if false, 1 if true
   */
  def booleanToByte(bool: Boolean): Byte =
    if (bool) 1 else 0

  /**
   * Helper to convert a Byte value
   * (1 or 0) into a Boolean.
   *
   * @param b The Byte to turn
   *        into a Boolean
   * @return the Boolean value of b, or
   *         an error message if b is
   *         not 0 or 1 - all boxed in a
   *         Scalaz Validation 
   */
  def byteToBoolean(b: Byte): Validation[String, Boolean] =
    if (b == 0)
      false.success
    else if (b == 1)
      true.success
    else
      "Cannot convert byte [%s] to boolean, only 1 or 0.".format(b).fail
}