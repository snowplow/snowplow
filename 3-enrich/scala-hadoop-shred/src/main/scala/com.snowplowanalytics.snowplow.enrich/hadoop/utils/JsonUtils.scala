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
package com.snowplowanalytics
package snowplow
package enrich
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

// Iglu Scala Client
import iglu.client.utils.ValidationExceptions

/**
 * Contains general purpose extractors and other
 * utilities for JSONs. Jackson-based.
 */
object JsonUtils {

  private lazy val Mapper = new ObjectMapper

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
      case e: Throwable => s"Field [$field]: invalid JSON [%s] with parsing error: %s".format(instance, ValidationExceptions.stripInstanceEtc(e.getMessage)).fail
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

}
