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

import cats.data.{NonEmptyList, Validated, ValidatedNel}
import cats.syntax.validated._
import com.snowplowanalytics.iglu.client.Resolver
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
   * @param etlVersion The ETL version
   * @param etlTstamp The ETL timestamp
   * @param input The ValidatedMaybeCanonicalInput
   * @param resolver (implicit) The Iglu resolver used for schema lookup and validation
   * @return the ValidatedMaybeCanonicalOutput. Thanks to flatMap, will include any validation
   * errors contained within the ValidatedMaybeCanonicalInput
   */
  def processEvents(
    adapterRegistry: AdapterRegistry,
    enrichmentRegistry: EnrichmentRegistry,
    etlVersion: String,
    etlTstamp: DateTime,
    input: ValidatedNel[String, Option[CollectorPayload]])(
    implicit resolver: Resolver
  ): List[ValidatedNel[String, EnrichedEvent]] = {
    def flattenToList[A](
      v: ValidatedNel[String, Option[ValidatedNel[String, NonEmptyList[ValidatedNel[String, A]]]]]
    ): List[ValidatedNel[String, A]] = v match {
      case Validated.Valid(Some(Validated.Valid(nel))) => nel.toList
      case Validated.Valid(Some(Validated.Invalid(f))) => List(f.invalid)
      case Validated.Invalid(f) => List(f.invalid)
      case Validated.Valid(None) => Nil
    }

    try {
      val e: ValidatedNel[
        String,
        Option[ValidatedNel[String, NonEmptyList[ValidatedNel[String, EnrichedEvent]]]]] =
        for {
          maybePayload <- input
        } yield
          for {
            payload <- maybePayload
          } yield
            for {
              events <- adapterRegistry.toRawEvents(payload)
            } yield
              for {
                event <- events
                enriched = EnrichmentManager.enrichEvent(enrichmentRegistry, etlVersion, etlTstamp, event)
              } yield enriched

      flattenToList[EnrichedEvent](e)
    } catch {
      case NonFatal(nf) => {
        val errorWriter = new StringWriter
        nf.printStackTrace(new PrintWriter(errorWriter))
        List(s"Unexpected error processing events: $errorWriter".invalidNel)
      }
    }
  }
}
