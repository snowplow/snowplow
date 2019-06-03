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

import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.specs2.mutable.Specification

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
    "remove the \"line\" field from a large bad JSON" in {
      val badJson = """{"line":"huge", "errors":["some error"], "other":"more information"}"""
      val parsed = parse(Source.adjustOversizedFailureJson(badJson))
      parsed \ "line" must_== JNothing
      parsed \ "other" must_== JString("more information")
      parsed \ "size" must_== JInt(Source.getSize(badJson))
    }

    "remove create a new bad row if the bad row JSON is unparseable" in {
      val badJson = "{"
      val parsed = parse(Source.adjustOversizedFailureJson(badJson))
      parsed \ "size" must_== JInt(1)
    }
  }

  "oversizedSuccessToFailure" should {
    "create a bad row JSON from an oversized success" in {
      val actual = parse(Source.oversizedSuccessToFailure("abc", 2))
      actual \ "size" must_== JInt(3)
      actual \ "errors" must_== JArray(
        List(
          JObject(
            List(
              ("level", JString("error")),
              (
                "message",
                JString("Enriched event size of 3 bytes is greater than allowed maximum of 2")
              )
            )
          )
        )
      )
    }
  }
}
