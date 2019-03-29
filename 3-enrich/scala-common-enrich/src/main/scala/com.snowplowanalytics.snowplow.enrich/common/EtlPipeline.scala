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

import java.io.{PrintWriter, StringWriter}

import scala.util.control.NonFatal

import cats.Monad
import cats.data.{EitherT, NonEmptyList, Validated, ValidatedNel}
import cats.effect.Clock
import cats.implicits._
import com.snowplowanalytics.iglu.client.Client
import com.snowplowanalytics.iglu.client.resolver.registries.RegistryLookup
import io.circe.Json
import org.joda.time.DateTime

import adapters.AdapterRegistry
import enrichments.{EnrichmentManager, EnrichmentRegistry}
import loaders.CollectorPayload
import outputs.EnrichedEvent

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
   * @param etlVersion The ETL version
   * @param etlTstamp The ETL timestamp
   * @param input The ValidatedMaybeCanonicalInput
   * @return the ValidatedMaybeCanonicalOutput. Thanks to flatMap, will include any validation
   * errors contained within the ValidatedMaybeCanonicalInput
   */
  def processEvents(
    adapterRegistry: AdapterRegistry,
    enrichmentRegistry: EnrichmentRegistry,
    client: Client[F, Json],
    etlVersion: String,
    etlTstamp: DateTime,
    input: ValidatedNel[String, Option[CollectorPayload]]
  ): F[List[ValidatedNel[String, EnrichedEvent]]] = {
    def flattenToList(
      v: ValidatedNel[String, Option[ValidatedNel[String, NonEmptyList[EnrichedEvent]]]]
    ): List[ValidatedNel[String, EnrichedEvent]] = v match {
      case Validated.Valid(Some(Validated.Valid(nel))) => nel.toList.map(_.valid)
      case Validated.Valid(Some(Validated.Invalid(f))) => List(f.invalid)
      case Validated.Invalid(f) => List(f.invalid)
      case Validated.Valid(None) => Nil
    }

    try {
      val e = for {
          maybePayload <- input
        } yield
          for {
            payload <- maybePayload
          } yield
            (for {
              events <- EitherT(adapterRegistry.toRawEvents(payload, client).map(_.toEither))
              enrichedEvents <-
                events.map { e =>
                  val r = EnrichmentManager
                    .enrichEvent(registry, client, etlVersion, etlTstamp, e).map(_.toEither)
                  EitherT(r)
                }.sequence
            } yield enrichedEvents).value.map(_.toValidated)

      e.map(_.sequence).sequence.map(flattenToList)
    } catch {
      case NonFatal(nf) => {
        val errorWriter = new StringWriter
        nf.printStackTrace(new PrintWriter(errorWriter))
        Monad[F].pure(List(s"Unexpected error processing events: $errorWriter".invalidNel))
      }
    }
  }
}
