/*
 * Copyright (c) 2012-2013 SnowPlow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.hadoop.hive

// Specs2
import org.specs2.mutable.Specification

class EventTypeTest extends Specification {

  // Valid event codes and their type mappings
  val eventCodes = Map(
    "ev" -> "struct",
    "se" -> "struct",
    "ad" -> "ad_impression",
    "tr" -> "transaction",
    "ti" -> "transaction_item",
    "pv" -> "page_view",
    "pp" -> "page_ping"
  )

  "A valid event code should be converted to its event type" >> {
    eventCodes foreach { case (code, typ) =>
      "The event code \"%s\"".format(code) should {
        "be convert to \"%s\"".format(typ) in {
          SnowPlowEventStruct.asEventType(code) must_== typ
        }
      }
    }
  }

  "An invalid event code \"oops\" " should {
    "throw an IllegalArgumentException" in {
      SnowPlowEventStruct.asEventType("oops") must throwA[IllegalArgumentException]
    }
  }
}
