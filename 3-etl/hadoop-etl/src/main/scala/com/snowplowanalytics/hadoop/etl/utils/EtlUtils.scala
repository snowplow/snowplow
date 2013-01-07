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
import java.net.URLDecoder;

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
   * robust output format (e.g. Avro)
   *
   * @param str The String to decode
   * @param encoding The encoding of the String
   * @return the decoded String (Option-boxed),
   *         or None if nothing to extract
   *
   * TODO: maybe a decoding failure should be
   * explicitly reported (e.g. via Either)
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
}