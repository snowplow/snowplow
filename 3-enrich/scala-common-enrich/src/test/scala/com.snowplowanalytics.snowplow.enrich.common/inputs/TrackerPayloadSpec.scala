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
package inputs

// Scalaz
import scalaz._
import Scalaz._

// Snowplow
import LoaderSpecHelpers._

// Specs2
import org.specs2.mutable.Specification
import org.specs2.matcher.DataTables
import org.specs2.scalaz.ValidationMatchers

class TrackerPayloadSpec extends Specification with DataTables with ValidationMatchers {

  "extractGetPayload" should {

    val Encoding = "UTF-8"

    // TODO: add more tests
    "return a Success-boxed NonEmptyList of NameValuePairs for a valid querystring" in {

      "SPEC NAME"                                   || "QUERYSTRING"                                                                    | "EXP. NEL"                                                                                                    |
      "Simple querystring #1"                       !! "e=pv&dtm=1376487150616&tid=483686"                                              ! toNameValueNel("e" -> "pv", "dtm" -> "1376487150616", "tid" -> "483686")                                      |
      "Simple querystring #2"                       !! "page=Celestial%2520Tarot%2520-%2520Psychic%2520Bazaar&vp=1097x482&ds=1097x1973" ! toNameValueNel("page" -> "Celestial%20Tarot%20-%20Psychic%20Bazaar", "vp" -> "1097x482", "ds" -> "1097x1973") |
      "Superfluous ? ends up in first param's name" !! "?e=pv&dtm=1376487150616&tid=483686"                                             ! toNameValueNel("?e" -> "pv", "dtm" -> "1376487150616", "tid" -> "483686")                                     |> {

        (_, qs, expected) => {
          TrackerPayload.extractGetPayload(qs.some, Encoding) must beSuccessful(expected)
        }
      }
    }

    "return a Failure message if the querystring is empty" in {
      TrackerPayload.extractGetPayload(None, Encoding) must beFailing("No name-value pairs extractable from querystring [] with encoding [UTF-8]")
    }

    // TODO: test invalid querystrings
  }
}
