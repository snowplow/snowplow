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
import cats.data.{Validated, ValidatedNel}
import cats.effect.Clock
import cats.implicits._

import io.circe.Json

import com.snowplowanalytics.iglu.client.Client
import com.snowplowanalytics.iglu.client.resolver.registries.RegistryLookup

import com.snowplowanalytics.snowplow.badrows.{BadRow, Processor}

import org.joda.time.DateTime

import com.snowplowanalytics.snowplow.enrich.common.adapters.AdapterRegistry
import com.snowplowanalytics.snowplow.enrich.common.enrichments.{EnrichmentManager, EnrichmentRegistry}
import com.snowplowanalytics.snowplow.enrich.common.loaders.CollectorPayload
import com.snowplowanalytics.snowplow.enrich.common.outputs.EnrichedEvent
import com.snowplowanalytics.snowplow.enrich.common.utils.HttpClient

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
  ): F[List[Validated[BadRow, EnrichedEvent]]] =
    input match {
      case Validated.Valid(Some(payload)) =>
        adapterRegistry
          .toRawEvents(payload, client, processor)
          .flatMap {
            case Validated.Valid(rawEvents) =>
              rawEvents.toList.traverse { event =>
                EnrichmentManager
                  .enrichEvent(
                    enrichmentRegistry,
                    client,
                    processor,
                    etlTstamp,
                    event
                  )
                  .toValidated
              }
            case Validated.Invalid(badRow) =>
              Monad[F].pure(List(badRow.invalid[EnrichedEvent]))
          }
      case Validated.Invalid(badRows) =>
        Monad[F].pure(badRows.map(_.invalid[EnrichedEvent])).map(_.toList)
      case Validated.Valid(None) =>
        Monad[F].pure(List.empty[Validated[BadRow, EnrichedEvent]])
    }
}
