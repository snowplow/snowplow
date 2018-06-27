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

import java.net.URI
import java.nio.file.{Files, Paths}

import org.scalatest._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import scalaz._

import common.outputs.{EnrichedEvent, BadRow}
import utils._

class UtilsSpec extends FreeSpec with Matchers {
  "the utils object should" - {
    "make a timeMs function available" - {
      "which report the time spent doing an operation" in {
        val (res, time) = timeMs((1L to 20L).foldRight(1L) { case (a, b) => a * b })
        res shouldBe 2432902008176640000L
        assert(time >= 1)
      }
    }
    "make a createSymLink function available" - {
      "which creates a symbolic link" in {
        val f = Files.createTempFile("test1", ".txt").toFile
        val s = "/tmp/test1.txt-symlink"
        createSymLink(f, s) shouldEqual Right(Paths.get(s))
        f.delete
        Paths.get(s).toFile.delete
      }
      "which fails if the symlink already exists" in {
        val f = Files.createTempFile("test2", ".txt").toFile
        createSymLink(f, f.toString) shouldEqual Left(s"Symlink ${f.toString} already exists")
        f.delete
      }
      "which fails if the symbolic link can't be created" in {
        val f = Files.createTempFile("test3", ".txt").toFile
        val s = "/proc/test3.txt-symlink"
        createSymLink(f, s) shouldEqual Left(s"Symlink can't be created: ${Paths.get(s)}")
        f.delete
      }
    }
    "make a getFilesToCache function available" - {
      "which sends back the files that need caching" in {
        val res = parse(SpecHelpers.resolverConfig)
        val reg = (("schema" -> "iglu:com.snowplowanalytics.snowplow/enrichments/jsonschema/1-0-0") ~
          ("data" -> List(parse(SpecHelpers.ipLookupsEnrichmentConfig))))
        getFilesToCache(res, reg) should contain only (
          (new URI("http://acme.com/GeoLite2-City.mmdb"), "./ip_geo"))
      }
    }
    "make a getEnrichedEventMetrics function available" - {
      "which sends back vendor and tracker metrics" in {
        val event = {
          val e = new EnrichedEvent()
          e.v_tracker = "go-1.0.0"
          e.event_vendor = "com.acme"
          e
        }
        getEnrichedEventMetrics(event) should contain only ("vendor_com_acme", "tracker_go_1_0_0")
      }
    }
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
