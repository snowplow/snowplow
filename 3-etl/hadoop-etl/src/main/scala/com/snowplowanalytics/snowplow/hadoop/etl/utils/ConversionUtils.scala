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
package com.snowplowanalytics.snowplow.hadoop.etl
package utils

// Java
import java.net.URLDecoder

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
   * @param str The String to decode
   * @param encoding The encoding of the String
   * @return a Scalaz Validation, wrapping either
   *         an error String or the decoded String
   */
  def decodeString(str: String, encoding: String): Validation[String, String] =
    try {
      val s = Option(str).getOrElse("")
      val d = URLDecoder.decode(s, encoding)
      val r = d.replaceAll("(\\r|\\n)", "")
               .replaceAll("\\t", "    ")
      r.success
    } catch {
      case e =>
        "Exception decoding [%s] from [%s] encoding: [%s]".format(str, encoding, e.getMessage).fail
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
   *         a Success Int
   */
  def stringToInt(str: String, field: String): Validation[String, Int] = try {
      str.toInt.success
    } catch {
      case nfe: NumberFormatException =>
        "Field [%s]: cannot convert [%s] to Int".format(field, str).fail
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
  def stringToByte(str: String, field: String): Validation[String, Byte] = try {
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
    if (str == "1") 
      true.success
    else if (str == "0")
      false.success
    else
      "Cannot convert [%s] to boolean, only 1 or 0.".format(str).fail

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