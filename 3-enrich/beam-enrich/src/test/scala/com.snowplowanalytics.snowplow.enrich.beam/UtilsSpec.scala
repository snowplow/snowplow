/*
 * Copyright (c) 2012-2019 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.enrich.beam

import java.nio.file.{Files, Paths}
import java.time.Instant

import com.snowplowanalytics.iglu.core.SelfDescribingData
import com.snowplowanalytics.snowplow.badrows._
import com.snowplowanalytics.snowplow.badrows.CPFormatViolationMessage._
import com.snowplowanalytics.snowplow.badrows.Failure.{CPFormatViolation, SizeViolation}
import com.snowplowanalytics.snowplow.badrows.Payload.RawPayload
import com.snowplowanalytics.snowplow.enrich.common.outputs.EnrichedEvent
import org.scalatest._

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
        createSymLink(f, f.toString) shouldEqual Left(s"A file at path ${f.toString} already exists")
        f.delete
      }
      "which fails if the symbolic link can't be created" in {
        val f = Files.createTempFile("test3", ".txt").toFile
        val s = "/proc/test3.txt-symlink"
        createSymLink(f, s) shouldEqual Left(s"Symlink can't be created: ${Paths.get(s)}")
        f.delete
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
        getSize("a" * 10) shouldEqual 10
      }
    }
    "make a resizeBadRow function available" - {
      "which leaves the bad row as is if it doesn't exceed the max size" in {
        val badRow = SelfDescribingData[BadRow](
          oversizedBadRow,
          BadRow(
            CPFormatViolation(
              Instant.ofEpochSecond(12),
              "tsv",
              FallbackCPFormatViolationMessage("ah")
            ),
            RawPayload("ah"),
            Processor("be", "1.0.0")
          )
        )
        resizeBadRow(badRow, 500, Processor("be", "1.0.0")) shouldEqual badRow
      }
      "which truncates the event in the bad row as is if it exceeds the max size" in {
        val original = SelfDescribingData[BadRow](
          oversizedBadRow,
          BadRow(
            CPFormatViolation(
              Instant.ofEpochSecond(12),
              "tsv",
              FallbackCPFormatViolationMessage("ah")
            ),
            RawPayload("ah"),
            Processor("be", "1.0.0")
          )
        )
        val res = resizeBadRow(original, 200, Processor("be", "1.0.0"))
        res.schema shouldEqual oversizedBadRow
        res.data.failure shouldBe a[SizeViolation]
        val f = res.data.failure.asInstanceOf[SizeViolation]
        f.actualSizeBytes shouldEqual 350
        f.maximumAllowedSizeBytes shouldEqual 200
        f.expectation shouldEqual "bad row exceeded the maximum size"
        resBr.payload shouldBe a[RawPayload]
        val p = resBr.payload.asInstanceOf[RawPayload]
        p.line shouldEqual "{\"schema\":\"iglu:com."
        resBr.processor shouldEqual Processor("be", "1.0.0")
      }
    }
    "make a resizeEnrichedEvent function available" - {
      "which truncates a formatted enriched event and wrap it in a bad row" in {
        val res = resizeEnrichedEvent("a" * 100, 100, 400, Processor("be", "1.0.0"))
        res.schema shouldEqual oversizedBadRow
        res.data.failure shouldBe a[SizeViolation]
        val f = res.data.failure.asInstanceOf[SizeViolation]
        f.actualSizeBytes shouldEqual 100
        f.maximumAllowedSizeBytes shouldEqual 400
        f.expectation shouldEqual "event passed enrichment but exceeded the maximum allowed size as a result"
        resBr.payload shouldBe a[RawPayload]
        val p = resBr.payload.asInstanceOf[RawPayload]
        p.line shouldEqual ("a" * 40)
        resBr.processor shouldEqual Processor("be", "1.0.0")
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
      "which filter the pii field" in {
        val event = {
          val e = new EnrichedEvent
          e.platform = "web"
          e.pii = "pii"
          e
        }
        tabSeparatedEnrichedEvent(event) should not include("pii")
      }
    }
    "make a getPii function available" - {
      "which return None if the pii field is null" in {
        val event = {
          val e = new EnrichedEvent
          e.platform = "web"
          e
        }
        getPiiEvent(event) shouldEqual None
      }
      "which return a new event if the pii field is present" in {
        val event = {
          val e = new EnrichedEvent
          e.pii = "pii"
          e.event_id = "id"
          e
        }
        val Some(e) = getPiiEvent(event)
        e.unstruct_event shouldEqual "pii"
        e.platform shouldEqual "srv"
        e.event shouldEqual "pii_transformation"
        e.event_vendor shouldEqual "com.snowplowanalytics.snowplow"
        e.event_format shouldEqual "jsonschema"
        e.event_name shouldEqual "pii_transformation"
        e.event_version shouldEqual "1-0-0"
        e.contexts should include
          """{"schema":"iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-0","data":[{"schema":"iglu:com.snowplowanalytics.snowplow/parent_event/jsonschema/1-0-0","data":{"parentEventId":"""
      }
    }
  }
}
