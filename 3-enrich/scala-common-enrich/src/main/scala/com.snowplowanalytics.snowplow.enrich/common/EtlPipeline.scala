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
import adapters.RawEvent
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
  def processEvents(registry: EnrichmentRegistry, etlVersion: String, etlTstamp: String, input: ValidatedMaybeCollectorPayload): ValidatedMaybeCanonicalOutput = {
    
    // TODO: move this into the adapter
    val rawEvent = for {
      mp <- input
    } yield for {
      p <- mp
      e = RawEvent(
        timestamp    = p.timestamp,
        vendor       = p.vendor,
        version      = p.version,
        parameters   = p.querystring.map(p => (p.getName -> p.getValue)).toList.toMap,
        // body:     String,
        source       = p.source,
        encoding     = p.encoding,
        ipAddress    = p.ipAddress,
        userAgent    = p.userAgent,
        refererUri   = p.refererUri,
        headers      = p.headers,
        userId       = p.userId
        )
    } yield e

    rawEvent.flatMap {
      _.cata(EnrichmentManager.enrichEvent(registry, etlVersion, etlTstamp, _).map(_.some),
             none.success)
    }
  }
}
