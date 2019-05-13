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
import com.snowplowanalytics.iglu.core.SchemaKey

sealed trait BadRow {
  def failure: Failure
  def payload: Payload
  def processor: Processor
}

sealed trait Failure

sealed trait Payload

final case class Processor(artifact: String, version: String)

final case class EnrichmentFailuresBadRow(
  failure: Failure,
  payload: Payload,
  processor: Processor
) extends BadRow

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
