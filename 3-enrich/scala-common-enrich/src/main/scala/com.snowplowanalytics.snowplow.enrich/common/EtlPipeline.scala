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

/**
 * Expresses the end-to-end event pipeline
 * supported by the Scala Common Enrich
 * project.
 */
object EtlPipeline {

  /**
   * A helper method to take a ValidatedMaybeCanonicalInput
   * and flatMap it into a ValidatedMaybeCanonicalOutput.
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
   * @return the ValidatedMaybeCanonicalOutput. Thanks to
   *         flatMap, will include any validation errors
   *         contained within the ValidatedMaybeCanonicalInput
   */
  def processEvents(registry: EnrichmentRegistry, etlVersion: String, etlTstamp: String, input: ValidatedMaybeCollectorPayload): ValidatedCanonicalOutputs = {

    def toRawEvents(maybePayload: MaybeCollectorPayload): ValidatedRawEvents =
      maybePayload.cata(AdapterRegistry.toRawEvents(_), Nil.success)

    type ListVld[A] = List[Validated[A]]
    def flattenToList[A](v: Validated[ListVld[A]]): ListVld[A] = v.fold(
      f => List(Failure(f)), s => s)

    flattenToList(for {
      maybePayload <- input
      events       <- toRawEvents(maybePayload)
    } yield for {
      event        <- events
      enriched      = EnrichmentManager.enrichEvent(registry, etlVersion, etlTstamp, event)
    } yield enriched)
  }
}
