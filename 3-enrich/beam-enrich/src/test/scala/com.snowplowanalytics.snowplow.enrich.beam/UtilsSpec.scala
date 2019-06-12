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

import com.snowplowanalytics.snowplow.badrows._
import com.snowplowanalytics.snowplow.enrich.common.outputs.EnrichedEvent
import cats.implicits._
import io.circe.parser
import com.snowplowanalytics.iglu.core.SelfDescribingData
import com.snowplowanalytics.iglu.core.circe.instances._

import org.scalatest._

import utils._

class UtilsSpec extends FreeSpec with Matchers {
  val processor = Processor("be", "1.0.0")

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
        val badRow = BadRow
          .CPFormatViolation(
            processor,
            Failure.CPFormatViolation(
              Instant.ofEpochSecond(12),
              "tsv",
              FailureDetails.CPFormatViolationMessage.Fallback("ah")
            ),
            Payload.RawPayload("ah")
          )
          .compact
        resizeBadRow(badRow, 500, processor) shouldEqual badRow
      }

      "which truncates the event in the bad row as is if it exceeds the max size" in {
        val original = BadRow
          .CPFormatViolation(
            processor,
            Failure.CPFormatViolation(
              Instant.ofEpochSecond(12),
              "tsv",
              FailureDetails.CPFormatViolationMessage.Fallback("ah")
            ),
            Payload.RawPayload("ah")
          )
          .compact

        val res = resizeBadRow(original, 150, processor)
        val resSdd = parseBadRow(res).right.get
        resSdd shouldBe a[BadRow.SizeViolation]
        val badRowSizeViolation = resSdd.asInstanceOf[BadRow.SizeViolation]
        badRowSizeViolation.failure.maximumAllowedSizeBytes shouldEqual 150
        badRowSizeViolation.failure.actualSizeBytes shouldEqual 267
        badRowSizeViolation.failure.expectation shouldEqual "bad row exceeded the maximum size"
        badRowSizeViolation.payload.line shouldEqual "{\"schema\":\"iglu"
        badRowSizeViolation.processor shouldEqual processor
      }
    }
    "make a resizeEnrichedEvent function available" - {
      "which truncates a formatted enriched event and wrap it in a bad row" in {
        val res = resizeEnrichedEvent("a" * 100, 100, 400, processor)
        val resSdd = parseBadRow(res).right.get
        resSdd shouldBe a[BadRow.SizeViolation]
        val badRowSizeViolation = resSdd.asInstanceOf[BadRow.SizeViolation]
        badRowSizeViolation.failure.maximumAllowedSizeBytes shouldEqual 400
        badRowSizeViolation.failure.actualSizeBytes shouldEqual 100
        badRowSizeViolation.failure.expectation shouldEqual "event passed enrichment but exceeded the maximum allowed size as a result"
        badRowSizeViolation.payload.line shouldEqual ("a" * 40)
        badRowSizeViolation.processor shouldEqual processor
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

  def parseBadRow(jsonStr: String): Either[String, BadRow] =
    for {
      json <- parser.parse(jsonStr).leftMap(_.getMessage)
      sdj <- SelfDescribingData.parse(json).leftMap(_.code)
      res <- sdj.data.as[BadRow].leftMap(_.getMessage)
    } yield res
}
