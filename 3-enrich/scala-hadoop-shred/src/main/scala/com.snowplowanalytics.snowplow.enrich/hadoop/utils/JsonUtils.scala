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
      case e: Throwable => s"Field [$field]: invalid JSON [%s] with parsing error: %s".format(instance, stripInstance(e.getMessage)).fail
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
   * Because this makes it hard to test.
   *
   * @param message The exception message which is
   *        leaking instance information
   * @return the same exception message, but with
   *         instance information removed
   */
  // TODO: get this out of Iglu Scala Client
  private[utils] def stripInstance(message: String): String = {
    message.replaceAll("@[0-9a-z]+;", "@xxxxxx;")
  }
}
