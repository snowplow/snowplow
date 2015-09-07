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
package com.snowplowanalytics.snowplow.enrich.common
package enrichments
package registry

// Specs2
import org.specs2.Specification
import org.specs2.scalaz.ValidationMatchers

// Scalaz
import scalaz._
import Scalaz._

/**
 * Tests EventFingerprintEnrichment
 */
class EventFingerprintEnrichmentSpec extends Specification with ValidationMatchers { def is =

  "This is a specification to test the EventFingerprintEnrichment"              ^
                                                                               p^
  "getEventFingerprint should combine fields into a hash"                       ! e1^
  "getEventFingerprint should not depend on the order of fields"                ! e2^
                                                                                end

  def e1 = {
    val config = EventFingerprintEnrichment(
      s => s.size.toString,
      List("stm")
    )

    config.getEventFingerprint(Map(
      "stm" -> "1000000000000",
      "e" -> "se",
      "se_ac" -> "buy"
      )) must_== "15"
  }

  def e2 = {
    val config = EventFingerprintEnrichment(EventFingerprintEnrichmentConfig.getAlgorithm("MD5").toOption.get, List("stm", "eid"))

    val initialVersion = Map(
      "stm" -> "1441630729922",
      "eid" -> "123e4567-e89b-12d3-a456-426655440000",
      "e" -> "se",
      "se_ac" -> "action",
      "se_ca" -> "category",
      "se_pr" -> "property"
      )

    val permutedVersion = Map(
      "stm" -> "1441630730000",
      "se_ca" -> "category",
      "se_ac" -> "action",
      "se_pr" -> "property",
      "e" -> "se"
      )

    config.getEventFingerprint(permutedVersion) must_== config.getEventFingerprint(initialVersion)
  }

}
