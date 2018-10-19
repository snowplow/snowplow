/*
 * Copyright (c) 2018 Snowplow Analytics Ltd. All rights reserved.
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

package com.snowplowanalytics.snowplow.enrich.badrows

import cats.data.NonEmptyList
import cats.syntax.show._

import io.circe.{Encoder, Json}
import io.circe.syntax._
import io.circe.generic.semiauto._

import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer, SelfDescribingData}
import com.snowplowanalytics.iglu.core.circe.instances._

import BadRow._

sealed trait BadRow {

  /** Everything necessary to recover/fix event(s) */
  def payload: BadRowPayload

  /** Application that produced bad row (Spark Enrich, Stream Enrich, Loader) */
  def processor: Processor

  /** Associated Iglu Schema */
  def schema: SchemaKey

  def toJson: SelfDescribingData[Json] =
    SelfDescribingData(this.schema, badRowEncoder(this))
}

object BadRow {
  // Placeholders
  type Processor         = String
  type ProcessingMessage = String // Schema validation message
  type Event             = String // Fully valid enriched event (from Analytics SDK)

  // Should be in iglu core
  implicit val schemaKeyEncoder: Encoder[SchemaKey] =
    Encoder.instance { schemaKey =>
      Json.fromString(schemaKey.show)
    }

  // Stages of pipeline
  type RawPayload = String // Format is valid
//  type CollectorPayload = Map[String, String] // Tracker Protocol is valid
  type SnowplowPayload = String // All iglu payloads aren't corrupted

  type EnrichedEvent = String // No failures during enrichment
  type CorrectEvent  = String // All schemas are valid

  val FormatViolationSchema =
    SchemaKey("com.snowplowanalytics.snowplow.badrows", "format_violation", "jsonschema", SchemaVer.Full(1, 0, 0))
  val TrackerProtocolViolationSchema = SchemaKey("com.snowplowanalytics.snowplow.badrows",
                                                 "tracker_protocol_violation",
                                                 "jsonschema",
                                                 SchemaVer.Full(1, 0, 0))
  val EnrichmentFailureSchema =
    SchemaKey("com.snowplowanalytics.snowplow.badrows", "enrichment_failure", "jsonschema", SchemaVer.Full(1, 0, 0))
  val SchemaInvalidationSchema =
    SchemaKey("com.snowplowanalytics.snowplow.badrows", "schema_invalidation", "jsonschema", SchemaVer.Full(1, 0, 0))
  val LoaderFailureSchema =
    SchemaKey("com.snowplowanalytics.snowplow.badrows", "loader_failure", "jsonschema", SchemaVer.Full(1, 0, 0))

  case class EnrichmentError(name: String, message: String)

  object EnrichmentError {
    implicit val encoder: Encoder[EnrichmentError] =
      deriveEncoder[EnrichmentError]
  }

  import IgluParseError._
  import IgluResolverError._

  /**
   * Zero validation level.
   * Completely invalid payload, malformed HTTP, something that cannot be parsed:
   * - truncation
   * - collector's bad row e.g. exceeded limit (processor will be "scala-collector")
   * - garbage
   * - invalid URL query string encoding
   * - robots
   *
   * Everything else if parsed into `RawPayload` could throw POST-bombs
   */
  case class FormatViolation(payload: BadRowPayload.RawCollectorBadRowPayload, message: String, processor: Processor)
      extends BadRow {
    def schema: SchemaKey = FormatViolationSchema
  }

  /**
   * First validation level.
   * Valid HTTP, but invalid Snowplow
   * - OPTIONS request
   * - unknown query parameters
   * - any properties supposed to be self-describing are not JSON (but still can be non iglu-compatible)
   * - any runtime exceptions happened before `EnrichmentManager`
   *
   * - missing iglu: protocol
   * - invalid iglu URI
   * - not self-describing data (missing `data` or `schema`)
   * - invalid SchemaVer
   *
   * @param payload payload is already splitted, but not yet validated as Snowplow event
   */
  case class TrackerProtocolViolation(payload: BadRowPayload.RawEventBadRowPayload,
                                      message: String,
                                      processor: Processor)
      extends BadRow {
    def schema: SchemaKey = TrackerProtocolViolationSchema
  }

  /**
   * Second validation level.
   * Any iglu protocol violation in a *single event*:
   *
   * @param payload single event payload
   */
  /**
   * Third validation level:
   * Runtime/validation errors in `EnrichmentManager`:
   * - runtime exception during Weather/lookup enrichments
   *
   * Usually can be just retried
   *
   * @param payload single event payload
   */
  case class EnrichmentFailure(payload: BadRowPayload.RawEvent,
                               errors: NonEmptyList[EnrichmentError],
                               processor: Processor)
      extends BadRow {
    def schema: SchemaKey = EnrichmentFailureSchema
  }

  /**
   * Third validation level:
   * Context or self-describing event is not valid against its schema.
   * Most common error-type.
   * - invalid property
   * - schema not found
   *
   * Technically, can happen before and after `enrich` because several enrichments
   * can add new schemas
   * @param payload single event payload (not just failed schema)
   */
  case class SchemaInvalidation(payload: BadRowPayload.RawEvent,
                                errors: NonEmptyList[IgluResolverError],
                                processor: Processor)
      extends BadRow {
    def schema: SchemaKey = SchemaInvalidationSchema
  }

  /**
   * Post-enrichment failure:
   * - shredder or BQ loader failed schema validation
   *
   * @param payload enriched event (TSV or JSON)
   */
  case class LoaderFailure(payload: BadRowPayload.Enriched,
                           errors: NonEmptyList[SelfDescribingData[Json]],
                           processor: Processor)
      extends BadRow {
    def schema: SchemaKey = LoaderFailureSchema
  }

  val badRowEncoder: Encoder[BadRow] = Encoder.instance {
    case FormatViolation(payload, message, processor) =>
      Json.fromFields(
        List("payload"   -> (payload: BadRowPayload).asJson,
             "message"   -> message.asJson,
             "processor" -> processor.asJson))
    case TrackerProtocolViolation(payload, message, processor) =>
      Json.fromFields(
        List("payload"   -> (payload: BadRowPayload).asJson,
             "message"   -> message.asJson,
             "processor" -> processor.asJson))
    case EnrichmentFailure(payload, errors, processor) =>
      Json.fromFields(
        List("payload" -> (payload: BadRowPayload).asJson, "errors" -> errors.asJson, "processor" -> processor.asJson))
    case SchemaInvalidation(payload, errors, processor) =>
      Json.fromFields(
        List("payload" -> (payload: BadRowPayload).asJson, "errors" -> errors.asJson, "processor" -> processor.asJson))
    case LoaderFailure(payload, errors, processor) =>
      Json.fromFields(
        List("payload" -> (payload: BadRowPayload).asJson, "errors" -> errors.asJson, "processor" -> processor.asJson))
  }
}
