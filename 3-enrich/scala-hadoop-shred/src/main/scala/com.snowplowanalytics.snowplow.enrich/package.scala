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

// Jackson
import com.fasterxml.jackson.databind.JsonNode

// JSON Schema Validator
import com.github.fge.jsonschema.core.report.ProcessingMessage

// Scalaz
import scalaz._
import Scalaz._

/**
 * Scala package object to hold types,
 * helper methods etc.
 *
 * See:
 * http://www.artima.com/scalazine/articles/package_objects.html
 */
package object hadoop {

  /**
   * Type alias for a `ValidationNel`
   * containing either error `ProcessingMessage`s
   * or a successfully validated `JsonNode`.
   */
  type RawValidatedJsonNode = ValidationNel[ProcessingMessage, JsonNode]

  /**
   * Type alias for a `ValidationNel`
   * containing either error `JsonNode`s
   * or a successfully validated `JsonNode`.
   */
  type ValidatedJsonNode = ValidationNel[JsonNode, JsonNode]

  /**
   * Type alias for a `ValidationNel` containing either
   * error `JsonNode`s or a single successfully
   * validated `JsonNode`.
   */
  type ValidatedJson = ValidationNel[JsonNode, JsonNode]

  /**
   * Wraps a `ValidatedJson` in an `Option`.
   */
  type MaybeValidatedJson = Option[ValidatedJson]

  /**
   * Type alias for a `ValidationNel` containing
   * either error `JsonNode`s or a List of successfully
   * validated `JsonNode`s.
   */
  type ValidatedJsonList = ValidationNel[JsonNode, List[JsonNode]]

  // TODO: need to fix this
  type SchemaVer = String
}
