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
import com.fasterxml.jackson.databind.JsonNode

// JSON Schema Validator
import com.github.fge.jsonschema.SchemaVersion
import com.github.fge.jsonschema.cfg.ValidationConfiguration
import com.github.fge.jsonschema.main.{
  JsonSchemaFactory,
  JsonValidator
}
import com.github.fge.jsonschema.core.report.{
  ListReportProvider,
  ProcessingMessage,
  LogLevel
}

// Scala
import scala.collection.JavaConversions._

// Scalaz
import scalaz._
import Scalaz._

// Snowplow Common Enrich
import common._

object ValidatableJsonNode {

  private lazy val JsonSchemaValidator = getJsonSchemaValidator(SchemaVersion.DRAFTV4)

  /**
   * Implicit to pimp a JsonNode to our
   * Scalaz Validation-friendly version.
   *
   * @param instance A JsonNode
   * @return the pimped ValidatableJsonNode
   */
  implicit def pimpJsonNode(instance: JsonNode) = new ValidatableJsonNode(instance)

  /**
   * Validates a JSON against a given
   * JSON Schema. On success, simply
   * passes through the original JSON.
   * On Failure, TODO
   *
   * @param instance The JSON to validate
   * @param schema The JSON Schema to
   *        validate the JSON against
   * @return either Success boxing the
   *         JsonNode, or a Failure boxing
   *         a NonEmptyList of
   *         ProcessingMessages
   */
  def validateAgainstSchema(instance: JsonNode, schema: JsonNode): ValidationNel[ProcessingMessage, JsonNode] = {
    val report = JsonSchemaValidator.validateUnchecked(schema, instance)
    val msgs = report.iterator.toList
    msgs match {
      case x :: xs if !report.isSuccess => NonEmptyList[ProcessingMessage](x, xs: _*).fail
      case Nil     if  report.isSuccess => instance.success
      case _                            => throw new FatalEtlError(s"Validation report success [$report.isSuccess] conflicts with message count [$msgs.length]")
    }
  }

  /**
   * Factory for retrieving a JSON Schema
   * validator with the specific version.
   *
   * @param version The version of the JSON
   *        Schema spec to validate against
   * @return a JsonValidator
   */
  private def getJsonSchemaValidator(version: SchemaVersion): JsonValidator = {
    
    // Override the ReportProvider so we never throw Exceptions and only collect ERRORS+
    val rep = new ListReportProvider(LogLevel.ERROR, LogLevel.NONE)
    val cfg = ValidationConfiguration
                .newBuilder
                .setDefaultVersion(version)
                .freeze
    val fac = JsonSchemaFactory
                .newBuilder
                .setReportProvider(rep)
                .setValidationConfiguration(cfg)
                .freeze
    
    fac.getValidator
  }
}

/**
 * A pimped JsonNode which supports validation
 * using JSON Schema.
 */
class ValidatableJsonNode(instance: JsonNode) {

  def validateAgainst(schema: JsonNode): ValidationNel[ProcessingMessage, JsonNode] = 
    ValidatableJsonNode.validateAgainstSchema(instance, schema)
}
