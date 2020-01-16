/*
 * Copyright (c) 2012-2020 Snowplow Analytics Ltd. All rights reserved.
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

import java.time.Instant

import cats.Monad
import cats.data.{EitherT, NonEmptyList, Validated, ValidatedNel}
import cats.effect.Clock
import cats.implicits._
import com.snowplowanalytics.iglu.client.Client
import com.snowplowanalytics.iglu.client.resolver.registries.RegistryLookup
import com.snowplowanalytics.snowplow.badrows._
import com.snowplowanalytics.snowplow.badrows.FailureDetails._
import io.circe.Json
import org.joda.time.DateTime

import adapters.{AdapterRegistry, RawEvent => RE}
import enrichments.{EnrichmentManager, EnrichmentRegistry}
import loaders.CollectorPayload
import outputs.EnrichedEvent
import utils.HttpClient

/** Expresses the end-to-end event pipeline supported by the Scala Common Enrich project. */
object EtlPipeline {

  /**
   * A helper method to take a ValidatedMaybeCanonicalInput and transform it into a List (possibly
   * empty) of ValidatedCanonicalOutputs.
   * We have to do some unboxing because enrichEvent expects a raw CanonicalInput as its argument,
   * not a MaybeCanonicalInput.
   * @param adapterRegistry Contains all of the events adapters
   * @param enrichmentRegistry Contains configuration for all enrichments to apply
   * @param client Our Iglu client, for schema lookups and validation
   * @param processor The ETL application (Spark/Beam/Stream enrich) and its version
   * @param etlTstamp The ETL timestamp
   * @param input The ValidatedMaybeCanonicalInput
   * @return the ValidatedMaybeCanonicalOutput. Thanks to flatMap, will include any validation
   * errors contained within the ValidatedMaybeCanonicalInput
   */
  def processEvents[F[_]: Monad: RegistryLookup: Clock: HttpClient](
    adapterRegistry: AdapterRegistry,
    enrichmentRegistry: EnrichmentRegistry[F],
    client: Client[F, Json],
    processor: Processor,
    etlTstamp: DateTime,
    input: ValidatedNel[BadRow, Option[CollectorPayload]]
  ): F[List[ValidatedNel[BadRow, EnrichedEvent]]] = {
    def flattenToList(
      v: ValidatedNel[BadRow, Option[ValidatedNel[BadRow, NonEmptyList[EnrichedEvent]]]]
    ): List[ValidatedNel[BadRow, EnrichedEvent]] =
      v match {
        case Validated.Valid(Some(Validated.Valid(nel))) => nel.toList.map(_.valid)
        case Validated.Valid(Some(Validated.Invalid(f))) => List(f.invalid)
        case Validated.Invalid(f) => List(f.invalid)
        case Validated.Valid(None) => Nil
      }

    val e = for {
      maybePayload <- input
    } yield for {
      payload <- maybePayload
    } yield (for {
      events <- EitherT(
        adapterRegistry
          .toRawEvents(payload, client, processor)
          .map(_.toEither.leftMap(sd => NonEmptyList.one(sd)))
      )
      enrichedEvents <- events.map { e =>
        val r = EnrichmentManager
          .enrichEvent(enrichmentRegistry, client, processor, etlTstamp, e)
          .map(_.toEither)
        EitherT(r).leftMap {
          case (nel, pee) => buildBadRows(nel, pee, RE.toRawEvent(e), processor)
        }
      }.sequence
    } yield enrichedEvents).value.map(_.toValidated)

    e.map(_.sequence).sequence.map(flattenToList)
  }

  def buildBadRows(
    nel: NonEmptyList[EnrichmentStageIssue],
    pee: Payload.PartiallyEnrichedEvent,
    re: Payload.RawEvent,
    processor: Processor
  ): NonEmptyList[BadRow] = {
    val (fs, vs) = splitIssues(nel)
    val brs = List(
      buildEnrichmentFailuresBadRow(fs, pee, re, processor),
      buildSchemaViolationsBadRow(vs, pee, re, processor)
    ).flatten
    // can't be empty
    NonEmptyList.fromList(brs).get
  }

  def buildEnrichmentFailuresBadRow(
    fs: List[FailureDetails.EnrichmentFailure],
    pee: Payload.PartiallyEnrichedEvent,
    re: Payload.RawEvent,
    processor: Processor
  ): Option[BadRow] =
    NonEmptyList
      .fromList(fs)
      .map { fs =>
        BadRow.EnrichmentFailures(
          processor,
          Failure.EnrichmentFailures(Instant.now(), fs),
          Payload.EnrichmentPayload(pee, re)
        )
      }

  def buildSchemaViolationsBadRow(
    vs: List[FailureDetails.SchemaViolation],
    pee: Payload.PartiallyEnrichedEvent,
    re: Payload.RawEvent,
    processor: Processor
  ): Option[BadRow] =
    NonEmptyList
      .fromList(vs)
      .map { vs =>
        BadRow.SchemaViolations(
          processor,
          Failure.SchemaViolations(Instant.now(), vs),
          Payload.EnrichmentPayload(pee, re)
        )
      }

  def splitIssues(
    n: NonEmptyList[EnrichmentStageIssue]
  ): (List[EnrichmentFailure], List[SchemaViolation]) =
    n.foldLeft((List.empty[EnrichmentFailure], List.empty[SchemaViolation])) {
      case ((es, ss), i) =>
        i match {
          case e: EnrichmentFailure => (e :: es, ss)
          case s: SchemaViolation => (es, s :: ss)
        }
    }
}
