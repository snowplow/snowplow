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
package com.snowplowanalytics.snowplow.enrich
package hadoop
package utils

// Jackson
import com.fasterxml.jackson.databind.{
  ObjectMapper,
  JsonNode
}

// Scalaz
import scalaz._
import Scalaz._

/**
 * Contains general purpose extractors and other
 * utilities for JSONs. Jackson-based.
 */
object JsonUtils {

  private lazy val Mapper = new ObjectMapper

  /**
   * Converts a JSON string into a Validation[String, Json]
   *
   * @param json The JSON string to parse
   * @return a Scalaz Validation, wrapping either an error
   *         String or the extracted JsonNode
   */
  def extractJson(field: String, str: String): Validation[String, JsonNode] =
    try {
      Mapper.readTree(str).success
    } catch {
      case e: Throwable => s"Field [$field]: invalid JSON [$str] with parsing error: $e.getMessage".fail
    }

}