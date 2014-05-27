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
package iglu

// Jackson
import com.github.fge.jackson.JsonLoader
import com.fasterxml.jackson.databind.JsonNode

// Scalaz
import scalaz._
import Scalaz._

/**
 * Provides access to an Iglu schema repository.
 *
 * This is an extremely primitive implementation.
 * Currently it only supports un-memoized access
 * to locally stored schemas specified by the
 * exact same version (i.e. MODEL-REVISION-ADDITION).
 */
object SchemaRepo {
  
  private val localPath = "/iglu-cache"

  // TODO: add in a mutable cache of JSON Schemas to prevent
  // a) lots of JsonNode instantiation and (later) b) lots
  // of unnecessary HTTP requests

  /**
   * Retrieves an IgluSchema from the Iglu Repo as
   * a JsonNode.
   *
   * @param schemaKey The SchemaKey uniquely identifies
   *        the schema in Iglu
   * @return a Validation boxing either the Schema's
   *         JsonNode on Success, or an error String
   *         on Failure 
   */
  // TODO: add error handling if this lookup fails
  def lookupSchema(schemaKey: SchemaKey): Validation[String, JsonNode] = {
    try {
      unsafeLookupSchema(schemaKey).success
    } catch {
      case e: Throwable => s"Cannot load schema ${schemaKey} from Iglu: ${e.getMessage}".fail
    }
  }

  /**
   * Retrieves an IgluSchema from the Iglu Repo as
   * a JsonNode. Unsafe.
   *
   * @param schemaKey The SchemaKey uniquely identifies
   *        the schema in Iglu
   * @return the JsonNode representing this schema
   */
  def unsafeLookupSchema(schemaKey: SchemaKey): JsonNode = {
    val schemaPath =  s"${localPath}/${schemaKey.toPath}"
    JsonLoader.fromResource(schemaPath)
  }

  /**
   * Retrieves an IgluSchema from the Iglu Repo as
   * a JsonNode. Convenience function which converts
   * an Iglu-format schema URI to a SchemaKey to
   * perform the lookup.
   */
  // def lookupSchema(schemaUri: SchemaUri): JsonNode
  // TODO: finish implementing when above method has
  // error handling
}
