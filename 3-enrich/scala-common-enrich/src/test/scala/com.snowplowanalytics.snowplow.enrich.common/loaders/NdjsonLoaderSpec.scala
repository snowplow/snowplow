/*
 * Copyright (c) 2015-2019 Snowplow Analytics Ltd. All rights reserved.
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
package loaders

import cats.data.NonEmptyList
import com.snowplowanalytics.snowplow.badrows._
import org.specs2.matcher.ValidatedMatchers
import org.specs2.mutable.Specification

class NdjsonLoaderSpec extends Specification with ValidatedMatchers {
  val Process = Processor("NdjsonLoaderSpec", "v1")

  "toCollectorPayload" should {
    "return failure on unparsable json" in {
      val invalid = NdjsonLoader("com.abc/v1").toCollectorPayload("""{ ... """, Process)
      invalid must beInvalid
    }

    "return success on parsable json" in {
      val valid = NdjsonLoader("com.abc/v1").toCollectorPayload("""{ "key": "value" }""", Process)
      valid must beValid
    }

    "return success with no content for empty rows" in {
      NdjsonLoader("com.abc/v1").toCollectorPayload("\r\n", Process) must beValid(None)
    }

    "fail if multiple lines passed in as one line" in {
      val line = List("""{"key":"value1"}""", """{"key":"value2"}""").mkString("\n")
      NdjsonLoader("com.abc/v1").toCollectorPayload(line, Process) must beInvalid.like {
        case NonEmptyList(
            BadRow.CPFormatViolation(
              Process,
              Failure.CPFormatViolation(_, "ndjson", f),
              Payload.RawPayload(l)
            ),
            List()
            ) =>
          f must_== FailureDetails.CPFormatViolationMessage.Fallback(
            "expected a single line, found 2"
          )
          l must_== line
      }
    }
  }
}
