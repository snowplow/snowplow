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

// Scalaz
import scalaz._
import Scalaz._

// Jackson
import com.fasterxml.jackson.databind.JsonNode

// JSON Schema Validator
import com.github.fge.jsonschema.SchemaVersion
import com.github.fge.jsonschema.cfg.ValidationConfiguration
import com.github.fge.jsonschema.main.{
  JsonSchemaFactory,
  JsonValidator
}

// Snowplow Common Enrich
import common._

object ValidatableJsonNode {

  private lazy val JsonSchemaValidator = getJsonSchemaValidator(SchemaVersion.DRAFTV4)

  /**
   * Implicit to pimp a JsonNode to our
   * Scalaz Validation-friendly version.
   *
   * @param jsonNode A node of JSON
   * @return the pimped ValidatableJsonNode
   */
  implicit def pimpJsonNode(jsonNode: JsonNode) = new ValidatableJsonNode(jsonNode)

  /**
   * Validates a JSON against a given
   * JSON Schema. On success, simply
   * passes through the original JSON.
   * On Failure, TODO
   *
   * @param json The JSON to validate
   * @param schema The JSON Schema to
   *        validate the JSON against
   *
   * @return either Success boxing the
   *         JSON, or a Failure boxing
   *         TODO
   */
  def validateAgainstSchema(json: JsonNode, schema: JsonNode): Validated[JsonNode] = {
    val report = JsonSchemaValidator.validate(schema, json)
    if (report.isSuccess) json.success else "OH NO".failNel
  }

  /**
   * Factory for retrieving a JSON Schema
   * validator with the specific version.
   *
   * @param version The version of the JSON
   *        Schema spec to validate against
   *
   * @return a JsonValidator
   */
  private def getJsonSchemaValidator(version: SchemaVersion): JsonValidator = {
    
    val cfg = ValidationConfiguration
                .newBuilder
                .setDefaultVersion(version)
                .freeze
    val fac = JsonSchemaFactory
                .newBuilder
                .setValidationConfiguration(cfg)
                .freeze
    
    fac.getValidator
  }
}

class ValidatableJsonNode(jsonNode: JsonNode) {

  def validate(schema: JsonNode): Validated[JsonNode] = 
    ValidatableJsonNode.validateAgainstSchema(jsonNode, schema)
}
