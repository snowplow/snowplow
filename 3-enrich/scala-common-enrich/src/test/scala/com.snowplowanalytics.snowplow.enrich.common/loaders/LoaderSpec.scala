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
package loaders

// Scalaz
import scalaz._
import Scalaz._

// Snowplow
import SpecHelpers._

// Specs2
import org.specs2.mutable.Specification
import org.specs2.matcher.DataTables
import org.specs2.scalaz.ValidationMatchers

object LoaderSpec {

  val loader = new Loader[String] {
    // Make our trait whole
    def toCollectorPayload(line: String): ValidatedMaybeCollectorPayload = "FAIL".failNel
  }
}

class LoaderSpec extends Specification with DataTables with ValidationMatchers {

  import LoaderSpec._

  "getLoader" should {

    "return the CloudfrontLoader" in {
      Loader.getLoader("cloudfront") must beSuccessful(CloudfrontLoader)
    }

    "return the CljTomcatLoader" in {
      Loader.getLoader("clj-tomcat") must beSuccessful(CljTomcatLoader)
    }

    "return the ThriftLoader" in {
      Loader.getLoader("thrift") must beSuccessful(ThriftLoader)
    }
  }

  "extractGetPayload" should {

    val Encoding = "UTF-8"

    // TODO: add more tests
    "return a Success-boxed NonEmptyList of NameValuePairs for a valid or empty querystring" in {

      "SPEC NAME"                                   || "QUERYSTRING"                                                                         | "EXP. NEL"                                                                                                      |
      "Simple querystring #1"                       !! "e=pv&dtm=1376487150616&tid=483686".some                                              ! toNameValuePairs("e" -> "pv", "dtm" -> "1376487150616", "tid" -> "483686")                                      |
      "Simple querystring #2"                       !! "page=Celestial%2520Tarot%2520-%2520Psychic%2520Bazaar&vp=1097x482&ds=1097x1973".some ! toNameValuePairs("page" -> "Celestial%20Tarot%20-%20Psychic%20Bazaar", "vp" -> "1097x482", "ds" -> "1097x1973") |
      "Superfluous ? ends up in first param's name" !! "?e=pv&dtm=1376487150616&tid=483686".some                                             ! toNameValuePairs("?e" -> "pv", "dtm" -> "1376487150616", "tid" -> "483686")                                     |
      "Empty querystring"                           !! None                                                                                  ! toNameValuePairs()                                                                                              |> {

        (_, qs, expected) => {
          loader.parseQuerystring(qs, Encoding) must beSuccessful(expected)
        }
      }
    }

    // TODO: test invalid querystrings
  }
}
