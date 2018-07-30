/*
 * Copyright (c) 2012-2018 Snowplow Analytics Ltd. All rights reserved.
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
class EventFingerprintEnrichmentSpec extends Specification with ValidationMatchers {
  def is = s2"""
  This is a specification to test the EventFingerprintEnrichment
  getEventFingerprint should combine fields into a hash                       $e1
  getEventFingerprint should not depend on the order of fields                $e2
  getEventFingerprint should not depend on excluded fields                    $e3
  getEventFingerprint should return different values even when fields overlap $e4
  """

  val standardConfig =
    EventFingerprintEnrichment(EventFingerprintEnrichmentConfig.getAlgorithm("MD5").toOption.get, List("stm", "eid"))

  def e1 = {
    val config = EventFingerprintEnrichment(
      s => s.size.toString,
      List("stm")
    )

    config.getEventFingerprint(
      Map(
        "stm"   -> "1000000000000",
        "e"     -> "se",
        "se_ac" -> "buy"
      )) must_== "15"
  }

  def e2 = {

    val initialVersion = Map(
      "e"     -> "se",
      "se_ac" -> "action",
      "se_ca" -> "category",
      "se_pr" -> "property"
    )

    val permutedVersion = Map(
      "se_ca" -> "category",
      "se_ac" -> "action",
      "se_pr" -> "property",
      "e"     -> "se"
    )

    standardConfig.getEventFingerprint(permutedVersion) must_== standardConfig.getEventFingerprint(initialVersion)
  }

  def e3 = {
    val initialVersion = Map(
      "stm"   -> "1000000000000",
      "eid"   -> "123e4567-e89b-12d3-a456-426655440000",
      "e"     -> "se",
      "se_ac" -> "buy"
    )
    val delayedVersion = Map(
      "stm"   -> "9999999999999",
      "e"     -> "se",
      "se_ac" -> "buy"
    )

    standardConfig.getEventFingerprint(delayedVersion) must_== standardConfig.getEventFingerprint(initialVersion)
  }

  def e4 = {
    val initialVersion = Map(
      "prefix" -> "suffix"
    )
    val overlappingVersion = Map("prefi" -> "xsuffix")

    standardConfig.getEventFingerprint(initialVersion) should not be standardConfig.getEventFingerprint(initialVersion)
  }

}
