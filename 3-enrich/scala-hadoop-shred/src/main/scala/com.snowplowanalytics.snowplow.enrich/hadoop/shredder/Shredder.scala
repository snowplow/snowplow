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
import iglu.SchemaRepo

/**
 * The shredder takes the two fields containing JSONs
 * (contexts and unstructured event properties) and
 * "shreds" their contents into a List of JsonNodes
 * ready for loading into dedicated tables in the
 * database.
 */
object Shredder {

  /**
   * Shred the CanonicalOutput's two fields which
   * contain JSONs: contexts and unstructured event
   * properties. By shredding we mean:
   *
   * 1. Verify the two fields contain valid JSONs
   * 2. Validate they conform to JSON Schema
   * 3. For the contexts, break the singular JsonNode
   *    into a List of individual context JsonNodes
   * 4. Collect the unstructured event and contexts
   *    into a singular List
   *
   * @param event The Snowplow enriched event to
   *        shred JSONs from
   * @return a Validation containing on Success a
   *         List (possible empty) of JsonNodes
   *         and on Failure a NonEmptyList of
   *         JsonNodes containing error messages
   */
  def shred(event: CanonicalOutput): ValidatedJsonList = {

    /*
    val unstructEvent = extractAndValidateJson(
      "ue_properties",
      Option(event.ue_properties),
      IgluRepo.Schemas.UnstructEvent)

    val contexts = extractAndValidateJson(
      "context",
      Option(event.contexts),
      IgluRepo.Schemas.Contexts) */

    // Placeholder for compilation
    JsonUtils.extractJson("todo", "[]").leftMap(e => JsonUtils.unsafeExtractJson(e)).map(j => List(j)).toValidationNel
  }

  /**
   * Extract the JSON from a String, and
   * validate it against the supplied
   * JSON Schema.
   *
   * @param field The name of the field
   *        containing the JSON instance
   * @param instance An Option-boxed JSON
   *        instance
   * @param schema The JSON Schema to
   *        validate this JSON instance
   *        against
   * @return an Option-boxed Validation
   *         containing either a Nel of
   *         JsonNodes error message on
   *         Failure, or a singular
   *         JsonNode on success
   */
  private[shredder] def extractAndValidateJson(field: String, instance: Option[String], schema: JsonNode): MaybeValidatedJson =
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
  private[shredder] def extractJson(field: String, instance: String): ValidatedJson =
    JsonUtils.extractJson(field, instance).leftMap { err =>
      JsonUtils.unsafeExtractJson(err)
    }.toValidationNel

  /**
   * Wrapper around ValidatableJsonNode's
   * validateAgainstSchema, to convert the
   * Failure Nel of ProcessingMessages to
   * a Failure Nel of Jsons.
   *
   * @param instance The JSON to validate
   * @param schema The JSON Schema to
   *        validate the JSON against
   * @return either Success boxing the
   *         JSON, or a Failure boxing
   *         a NonEmptyList of JsonNodes
   */
  private[shredder] def validateAgainstSchema(instance: JsonNode, schema: JsonNode): ValidatedJsonNode =
    ValidatableJsonNode.validateAgainstSchema(instance, schema).leftMap {
      _.map(_.asJson)
    }
}
