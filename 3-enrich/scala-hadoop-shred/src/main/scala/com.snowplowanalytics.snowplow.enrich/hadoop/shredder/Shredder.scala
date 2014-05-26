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
package shredder

// Jackson
import com.github.fge.jackson.JsonLoader
import com.fasterxml.jackson.databind.{
  JsonNode
}

// Scalaz
import scalaz._
import Scalaz._

// Snowplow Common Enrich
import common._
import outputs.CanonicalOutput

// This project
import hadoop.utils.{
  JsonUtils,
  ValidatableJsonNode
}

/**
 * The shredder takes the two fields containing JSONs
 * (contexts and unstructured event properties) and
 * "shreds" their contents into a List of JsonNodes
 * ready for loading into dedicated tables in the
 * database.
 */
object Shredder {

  private object Schemas {
    private val path = "/jsonschema/com.snowplowanalytics.snowplow"
    val Contexts = JsonLoader.fromResource(s"$path/contexts.json")
    val UnstructEvent = JsonLoader.fromResource(s"$path/unstruct_event.json")
  }

  def shred(event: CanonicalOutput): ValidatedJsonList = {

    val contexts = extractAndValidateJson("context", Option(event.contexts), Schemas.Contexts)
    val unstructEvent = extractAndValidateJson("ue_properties", Option(event.ue_properties), Schemas.UnstructEvent)

    // Placeholder for compilation
    JsonUtils.extractJson("todo", "[]").leftMap(e => JsonUtils.unsafeExtractJson(e)).map(j => List(j)).toValidationNel
  }

  private def extractAndValidateJson(field: String, instance: Option[String], schema: JsonNode): MaybeValidatedJson =
    instance.map { i =>
      val json = extractJson(field, i)
      json.flatMap(j => validateAgainstSchema(j, schema))
    }

  /**
   * Wrapper around JsonUtils' extractJson which
   * converts the failure to a JsonNode Nel, for
   * compatibility with subsequent JSON Schema
   * checks.
   *
   * @param field The name of the field
   *        containing JSON
   * @param instance The JSON instance itself
   * @return the pimped ScalazArgs
   */
  private def extractJson(field: String, instance: String): ValidatedJson =
    JsonUtils.extractJson(field, instance).leftMap { err =>
      JsonUtils.unsafeExtractJson(err)
    }.toValidationNel

  // Convert the Nel of ProcessingMessages to Jsons
  def validateAgainstSchema(instance: JsonNode, schema: JsonNode): ValidatedJsonNode =
    ValidatableJsonNode.validateAgainstSchema(instance, schema).leftMap {
      _.map(_.asJson)
    }
}
