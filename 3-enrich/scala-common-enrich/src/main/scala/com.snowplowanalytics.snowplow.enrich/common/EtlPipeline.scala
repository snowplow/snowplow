/*
 * Copyright (c) 2012-2014 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics
package snowplow
package enrich
package common

// Iglu
import iglu.client.Resolver

// Scalaz
import scalaz._
import Scalaz._

// This project
import adapters.{
  RawEvent,
  AdapterRegistry
}
import enrichments.{
  EnrichmentRegistry,
  EnrichmentManager
}
import outputs.EnrichedEvent

/**
 * Expresses the end-to-end event pipeline
 * supported by the Scala Common Enrich
 * project.
 */
object EtlPipeline {

  /**
   * A helper method to take a ValidatedMaybeCanonicalInput
   * and transform it into a List (possibly empty) of
   * ValidatedCanonicalOutputs.
   *
   * We have to do some unboxing because enrichEvent
   * expects a raw CanonicalInput as its argument, not
   * a MaybeCanonicalInput.
   *
   * @param registry Contains configuration for all
   *        enrichments to apply
   * @param etlVersion The ETL version
   * @param etlTstamp The ETL timestamp
   * @param input The ValidatedMaybeCanonicalInput
   * @param resolver (implicit) The Iglu resolver used for
   *        schema lookup and validation
   * @return the ValidatedMaybeCanonicalOutput. Thanks to
   *         flatMap, will include any validation errors
   *         contained within the ValidatedMaybeCanonicalInput
   */
  def processEvents(registry: EnrichmentRegistry, etlVersion: String, etlTstamp: String, input: ValidatedMaybeCollectorPayload)(implicit resolver: Resolver): List[ValidatedEnrichedEvent] = {

    def flattenToList[A](v: Validated[Option[Validated[NonEmptyList[Validated[A]]]]]): List[Validated[A]] = v match {
      case Success(Some(Success(nel))) => nel.toList
      case Success(Some(Failure(f)))   => List(f.fail)
      case Failure(f)                  => List(f.fail)
      case Success(None)               => Nil
    }

    val e: Validated[Option[Validated[NonEmptyList[ValidatedEnrichedEvent]]]] =
      for {
        maybePayload  <- input
      } yield for {
        payload       <- maybePayload
      } yield for {
        events        <- AdapterRegistry.toRawEvents(payload)
      } yield for {
        event         <- events
        enriched       = EnrichmentManager.enrichEvent(registry, etlVersion, etlTstamp, event)
      } yield enriched

    flattenToList[EnrichedEvent](e)
  }
}
