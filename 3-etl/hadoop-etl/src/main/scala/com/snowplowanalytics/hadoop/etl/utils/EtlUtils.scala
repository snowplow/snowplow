/*
 * Copyright (c) 2012 SnowPlow Analytics Ltd. All rights reserved.
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
object EtlUtils {

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
   * TODO: maybe a decoding failure should be
   * explicitly reported (e.g. via Either)
   *
   * @param str The String to decode
   * @param encoding The encoding of the String
   * @return the decoded String (Option-boxed),
   *         or None if nothing to extract
   */
  def decodeSafely(str: String, encoding: String): Option[String] =
    try {
      for {
        s <- Option(str)
        d = URLDecoder.decode(s, encoding)
        if d != null
        r = d.replaceAll("(\\r|\\n)", "")
             .replaceAll("\\t", "    ")
      } yield r
    } catch {
      case _ => None
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
}