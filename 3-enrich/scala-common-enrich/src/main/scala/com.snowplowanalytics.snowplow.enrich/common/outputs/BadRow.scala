/*
 * Copyright (c) 2014-2019 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.enrich.common.outputs

import java.time.ZonedDateTime

import cats.data.NonEmptyList
import com.snowplowanalytics.iglu.client.ClientError
import com.snowplowanalytics.iglu.core.{ParseError, SchemaCriterion, SchemaKey}

sealed trait BadRow {
  def failure: Failure
  def payload: Payload
  def processor: Processor
}

sealed trait Failure

sealed trait Payload

final case class Processor(artifact: String, version: String)

// COLLECTOR PAYLOAD FORMAT VIOLATION

final case class CollectorPayloadFormatViolation(
  timestamp: ZonedDateTime,
  loader: String,
  message: CPFormatViolationMessage
) extends Failure

sealed trait CPFormatViolationMessage
final case class InputDataCPFormatViolationMessage(
  payloadField: String,
  value: Option[String],
  expectation: String
) extends CPFormatViolationMessage
final case class FallbackCPFormatViolationMessage(error: String) extends CPFormatViolationMessage

// ADAPTER FAILURES

final case class AdapterFailures(
  timestamp: ZonedDateTime,
  vendor: String,
  version: String,
  messages: NonEmptyList[AdapterFailure]
) extends Failure

sealed trait AdapterFailure
// tracker protocol
final case class NotJsonAdapterFailure(
  field: String,
  json: String,
  error: String
) extends AdapterFailure
final case class NotSDAdapterFailure(json: String, error: ParseError) extends AdapterFailure
final case class IgluErrorAdapterFailure(schemaKey: SchemaKey, error: ClientError)
    extends AdapterFailure
final case class SchemaCritAdapterFailure(schemaKey: SchemaKey, schemaCriterion: SchemaCriterion)
    extends AdapterFailure
// webhook adapters
final case class SchemaMappingAdapterFailure(
  actual: Option[String],
  expectedMapping: Map[String, String],
  expectation: String
) extends AdapterFailure
final case class InputDataAdapterFailure(
  field: String,
  value: Option[String],
  expectation: String
) extends AdapterFailure

// SCHEMA VIOLATIONS

final case class SchemaViolations(timestamp: ZonedDateTime, messages: NonEmptyList[SchemaViolation])
    extends Failure

sealed trait SchemaViolation
final case class NotJsonSchemaViolation(
  field: String,
  json: String,
  error: String
) extends SchemaViolation
final case class NotSDSchemaViolation(json: String, error: ParseError) extends SchemaViolation
final case class IgluErrorSchemaViolation(schemaKey: SchemaKey, error: ClientError)
    extends SchemaViolation
final case class SchemaCritSchemaViolation(schemaKey: SchemaKey, schemaCriterion: SchemaCriterion)
    extends SchemaViolation

// ENRICHMENT FAILURES

final case class EnrichmentFailures(
  timestamp: ZonedDateTime,
  messages: NonEmptyList[EnrichmentFailure]
) extends Failure

final case class EnrichmentFailure(
  enrichment: EnrichmentInformation,
  message: EnrichmentFailureMessage
)

sealed trait EnrichmentFailureMessage
final case class SimpleEnrichmentFailureMessage(error: String) extends EnrichmentFailureMessage
final case class InputDataEnrichmentFailureMessage(
  field: String,
  value: Option[String],
  expectation: String
) extends EnrichmentFailureMessage

final case class EnrichmentInformation(schemaKey: SchemaKey, identifier: String)
