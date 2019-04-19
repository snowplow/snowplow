/*
 * Copyright (c) 2017-2019 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.enrich.common
package enrichments.registry

import com.fasterxml.jackson.databind.{ObjectMapper, SerializationFeature}
import com.jayway.jsonpath.spi.json.JacksonJsonNodeJsonProvider
import com.jayway.jsonpath.{Configuration, Option => JOption}

import outputs.EnrichedEvent

package object pii {
  type DigestFunction = Function1[Array[Byte], String]
  type ModifiedFields = List[ModifiedField]
  type ApplyStrategyFn = (String, PiiStrategy) => (String, ModifiedFields)
  type MutatorFn = (EnrichedEvent, PiiStrategy, ApplyStrategyFn) => ModifiedFields

  val JsonMutators = Mutators.JsonMutators
  val ScalarMutators = Mutators.ScalarMutators

  // Configuration for JsonPath, SerializationFeature.FAIL_ON_EMPTY_BEANS is required otherwise an
  // invalid path causes an exception
  private[pii] val JacksonNodeJsonObjectMapper = {
    val objectMapper = new ObjectMapper()
    objectMapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false)
    objectMapper
  }
  // SUPPRESS_EXCEPTIONS is useful here as we prefer an empty list to an exception when a path is
  // not found.
  private[pii] val JsonPathConf =
    Configuration
      .builder()
      .options(JOption.SUPPRESS_EXCEPTIONS)
      .jsonProvider(new JacksonJsonNodeJsonProvider(JacksonNodeJsonObjectMapper))
      .build()
}

package pii {

  /**
   * PiiStrategy trait. This corresponds to a strategy to apply to a single field. Currently only
   * String input is supported.
   */
  trait PiiStrategy {
    def scramble(clearText: String): String
  }

  /**
   * The mutator class encapsulates the mutator function and the field name where the mutator will
   * be applied.
   */
  private[pii] final case class Mutator(fieldName: String, muatatorFn: MutatorFn)

  /**
   * Parent class for classes that serialize the values that were modified during the PII enrichment
   */
  private[pii] final case class PiiModifiedFields(
    modifiedFields: ModifiedFields,
    strategy: PiiStrategy
  )

  /** Case class for capturing scalar field modifications. */
  private[pii] final case class ScalarModifiedField(
    fieldName: String,
    originalValue: String,
    modifiedValue: String
  ) extends ModifiedField

  /** Case class for capturing JSON field modifications. */
  private[pii] final case class JsonModifiedField(
    fieldName: String,
    originalValue: String,
    modifiedValue: String,
    jsonPath: String,
    schema: String
  ) extends ModifiedField

  /**
   * PiiField trait. This corresponds to a configuration top-level field (i.e. either a scalar or a
   * JSON field) along with a function to apply that strategy to the EnrichedEvent POJO (A scalar
   * field is represented in config py "pojo")
   */
  trait PiiField {

    /**
     * The POJO mutator for this field
     * @return fieldMutator
     */
    def fieldMutator: Mutator

    /**
     * Gets an enriched event from the enrichment manager and modifies it according to the specified
     * strategy.
     * @param event The enriched event
     */
    def transform(event: EnrichedEvent, strategy: PiiStrategy): ModifiedFields =
      fieldMutator.muatatorFn(event, strategy, applyStrategy)

    protected def applyStrategy(fieldValue: String, strategy: PiiStrategy): (String, ModifiedFields)
  }

  /**
   * The modified field trait represents an item that is transformed in either the JSON or a scalar
   * mutators.
   */
  sealed trait ModifiedField

  /** Abstract type to get salt using the supported methods */
  private[pii] trait PiiStrategyPseudonymizeSalt {
    def getSalt: String
  }
}
