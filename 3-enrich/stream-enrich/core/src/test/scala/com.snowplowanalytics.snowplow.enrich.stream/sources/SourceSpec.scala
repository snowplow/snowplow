/*
 * Copyright (c) 2013-2019 Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0, and
 * you may not use this file except in compliance with the Apache License
 * Version 2.0.  You may obtain a copy of the Apache License Version 2.0 at
 * http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the Apache License Version 2.0 is distributed on an "AS
 * IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the Apache License Version 2.0 for the specific language
 * governing permissions and limitations there under.
 */
package com.snowplowanalytics.snowplow.enrich.stream
package sources

import java.time.Instant

import org.specs2.mutable.Specification
import com.snowplowanalytics.snowplow.badrows._

class SourceSpec extends Specification {

  "getSize" should {
    "get the size of a string of ASCII characters" in {
      Source.getSize("abcdefg") must_== 7
    }

    "get the size of a string containing non-ASCII characters" in {
      Source.getSize("™®字") must_== 8
    }
  }

  "adjustOversizedFailureJson" should {
    "truncate the original bad row" in {
      val processor = Processor("se", "1.0.0")
      val original = BadRow.CPFormatViolation(
        processor,
        Failure.CPFormatViolation(
          Instant.ofEpochSecond(12),
          "tsv",
          FailureDetails.CPFormatViolationMessage.Fallback("ah")
        ),
        Payload.RawPayload("ah")
      )
      val res = Source.adjustOversizedFailureJson(original, 200, processor)
      res.schemaKey must_== Schemas.SizeViolation
      val failure = res.failure
      failure.actualSizeBytes must_== 267
      failure.maximumAllowedSizeBytes must_== 200
      failure.expectation must_== "bad row exceeded the maximum size"
      res.payload must_== Payload.RawPayload("""{"schema":"iglu:com.""")
      res.processor must_== processor
    }
  }

  "oversizedSuccessToFailure" should {
    "create a bad row JSON from an oversized success" in {
      val processor = Processor("se", "1.0.0")
      val res =
        Source.oversizedSuccessToFailure("abcdefghijklmnopqrstuvwxy", 10, processor)
      res.schemaKey must_== Schemas.SizeViolation
      val failure = res.failure
      failure.actualSizeBytes must_== 25
      failure.maximumAllowedSizeBytes must_== 10
      failure.expectation must_== "event passed enrichment but exceeded the maximum allowed size as a result"
      res.payload must_== Payload.RawPayload("a")
      res.processor must_== processor
    }
  }
}
