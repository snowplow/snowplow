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

import cats.Monad
import cats.data.{EitherT, NonEmptyList, Validated, ValidatedNel}
import cats.effect.Clock
import cats.implicits._
import com.snowplowanalytics.iglu.client.Client
import com.snowplowanalytics.iglu.client.resolver.registries.RegistryLookup
import com.snowplowanalytics.snowplow.badrows._
import io.circe.Json
import org.joda.time.DateTime

import adapters.AdapterRegistry
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
          .map(_.toValidatedNel.toEither)
      )
      enrichedEvents <- events.map { e =>
        EitherT(
          EnrichmentManager
            .enrichEvent(enrichmentRegistry, client, processor, etlTstamp, e)
            .value
            .map(_.toEitherNel)
        )
      }.sequence
    } yield enrichedEvents).value.map(_.toValidated)

    e.map(_.sequence).sequence.map(flattenToList)
  }
}
