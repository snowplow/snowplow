/*
 * Copyright (c) 2012-2018 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at
 * http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and
 * limitations there under.
 */
package com.snowplowanalytics
package snowplow.enrich
package beam

import org.scalatest._
import Matchers._
import scalaz._

import common.outputs.{EnrichedEvent, BadRow}
import utils._

class UtilsSpec extends FreeSpec {
  "the utils object should" - {
    "make a getStringSize function available" - {
      "which sends back the size of a string in bytes" in {
        getStringSize("a" * 10) shouldEqual 10
      }
    }
    "make a resizeBadRow function available" - {
      "which leaves the bad row as is if it doesn't exceed the max size" in {
        val badRow = BadRow("abc", NonEmptyList("error"))
        resizeBadRow(badRow, 10) shouldEqual badRow
      }
      "which truncates the event in the bad row as is if it exceeds the max size" in {
        val badRow = BadRow("a" * 100, NonEmptyList("error"))
        val resizedBadRow = resizeBadRow(badRow, 40)
        resizedBadRow.line shouldEqual "a"
        resizedBadRow.errors.map(_.getMessage) shouldEqual NonEmptyList(
          "Size of bad row (100) is greater than allowed maximum size (40)",
          "error"
        )
      }
    }
    "make a resizeEnrichedEvent function available" - {
      "which truncates a formatted enriched event and wrap it in a bad row" in {
        val badRow = resizeEnrichedEvent("a" * 100, 100, 400)
        badRow.line shouldEqual "a" * 10
        badRow.errors.map(_.getMessage) shouldEqual NonEmptyList(
          "Size of enriched event (100) is greater than allowed maximum (400)"
        )
      }
    }
    "make a tabSeparatedEnrichedEvent function available" - {
      "which tsv format an enriched event" in {
        val event = {
          val e = new EnrichedEvent
          e.platform = "web"
          e
        }
        tabSeparatedEnrichedEvent(event) should include("web")
      }
    }
  }
}
