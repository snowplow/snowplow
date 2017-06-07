/*
 * Copyright (c) 2015 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.enrich.common.loaders

import com.snowplowanalytics.snowplow.enrich.common.ValidatedMaybeCollectorPayload

// Scalaz
import scalaz._
import Scalaz._

// json4s
import org.json4s._
import org.json4s.jackson.JsonMethods._

// Java
import com.fasterxml.jackson.core.JsonParseException


case class NdjsonLoader(adapter: String) extends Loader[String] {

  private val CollectorName = "ndjson"
  private val CollectorEncoding = "UTF-8"

  /**
   * Converts the source string into a
   * CanonicalInput.
   *
   * @param line A line of data to convert
   * @return a CanonicalInput object, Option-
   *         boxed, or None if no input was
   *         extractable.
   */
  override def toCollectorPayload(line: String): ValidatedMaybeCollectorPayload = {

    try {

      if (line.replaceAll("\r?\n", "").isEmpty) {
        Success(None)
      } else if (line.split("\r?\n").size > 1) {
        "Too many lines! Expected single line".failNel
      } else {
        parse(line)
        CollectorApi.parse(adapter).map(
          CollectorPayload(
            Nil,
            CollectorName,
            CollectorEncoding,
            None,
            None,
            None,
            None,
            None,
            Nil,
            None,
            _,
            None,
            Some(line)
          ).some
        ).toValidationNel
      }

    } catch {
      case e: JsonParseException => "Unparsable JSON".failNel
    }
  }

}
