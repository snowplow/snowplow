/**
 * Copyright 2012-2013 Snowplow Analytics Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.snowplowanalytics.refererparser.scala

// Specs2
import org.specs2.mutable.Specification

class OtherReferalTest extends Specification {

  val refererUrl = "http://www.facebook.com"
  val pageUrl = "http://www.psychicbazaar.com"

  "A non-search referal (e.g. Facebook)" should {
    val referer = Parser.parse(refererUrl, pageUrl).get

    "have referer medium set to UNKNOWN" in {
      referer.medium must_== Medium.Unknown
    }
    "have no search information" in {
      referer.term must beNone
    }
  }
}