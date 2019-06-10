/*
 * Copyright (c) 2012-2019 Snowplow Analytics Ltd. All rights reserved.
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
import com.snowplowanalytics.iglu.core._
import com.snowplowanalytics.snowplow.badrows._
import com.snowplowanalytics.snowplow.badrows.Failure._
import com.snowplowanalytics.snowplow.badrows.Payload._
import io.circe.Json
import org.joda.time.DateTime

import adapters.AdapterRegistry
import enrichments.{EnrichmentManager, EnrichmentRegistry}
import loaders.CollectorPayload
import outputs.EnrichedEvent

/** Expresses the end-to-end event pipeline supported by the Scala Common Enrich project. */
object EtlPipeline {

  private type BR = SelfDescribingData[BadRow]

  /**
   * A helper method to take a ValidatedMaybeCanonicalInput and transform it into a List (possibly
   * empty) of ValidatedCanonicalOutputs.
   * We have to do some unboxing because enrichEvent expects a raw CanonicalInput as its argument,
   * not a MaybeCanonicalInput.
   * @param adapterRegistry Contains all of the events adapters
   * @param enrichmentRegistry Contains configuration for all enrichments to apply
   * @param client Our Iglu client, for schema lookups and validation
   * @param etlVersion The ETL version
   * @param etlTstamp The ETL timestamp
   * @param input The ValidatedMaybeCanonicalInput
   * @return the ValidatedMaybeCanonicalOutput. Thanks to flatMap, will include any validation
   * errors contained within the ValidatedMaybeCanonicalInput
   */
  def processEvents(
    adapterRegistry: AdapterRegistry,
    enrichmentRegistry: EnrichmentRegistry[F],
    client: Client[F, Json],
    processor: Processor,
    etlTstamp: DateTime,
    input: ValidatedNel[BR, Option[CollectorPayload]]
  ): F[List[ValidatedNel[BR, EnrichedEvent]]] = {
    def flattenToList(
      v: ValidatedNel[BR, Option[ValidatedNel[BR, NonEmptyList[EnrichedEvent]]]]
    ): List[ValidatedNel[BR, EnrichedEvent]] = v match {
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
          .enrichEvent(registry, client, processor, etlTstamp, e)
          .map(_.toEither)
        EitherT(r).leftMap { case (nel, pee) => buildBadRows(nel, pee, processor) }
      }.sequence
    } yield enrichedEvents).value.map(_.toValidated)

    e.map(_.sequence).sequence.map(flattenToList)
  }

  def buildBadRows(
    nel: NonEmptyList[EnrichmentStageIssue],
    pee: PartiallyEnrichedEvent,
    processor: Processor
  ): NonEmptyList[BR] = {
    val (fs, vs) = splitIssues(nel)
    val brs = List(
      buildEnrichmentBadRow(fs, pee, processor),
      buildSchemaBadRow(vs, pee, processor)
    ).flatten
    // can't be empty
    NonEmptyList.fromList(brs).get
  }

  def buildEnrichmentBadRow(
    fs: List[EnrichmentFailure],
    pee: PartiallyEnrichedEvent,
    processor: Processor
  ): Option[BR] = {
    val schemaKey = SchemaKey(
      "com.snowplowanalytics.snowplow.badrows",
      "enrichment_failures",
      "jsonschema",
      SchemaVer.Full(1, 0, 0)
    )
    NonEmptyList
      .fromList(fs)
      .map { fs =>
        val br = BadRow(
          EnrichmentFailures(Instant.now(), fs),
          pee,
          processor
        )
        SelfDescribingData(schemaKey, br)
      }
  }

  def buildSchemaBadRow(
    vs: List[SchemaViolation],
    pee: PartiallyEnrichedEvent,
    processor: Processor
  ): Option[BR] = {
    val schemaKey = SchemaKey(
      "com.snowplowanalytics.snowplow.badrows",
      "schema_violations",
      "jsonschema",
      SchemaVer.Full(1, 0, 0)
    )
    NonEmptyList
      .fromList(vs)
      .map { vs =>
        val br = BadRow(
          SchemaViolations(Instant.now(), vs),
          pee,
          processor
        )
        SelfDescribingData(schemaKey, br)
      }
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
