/**
 * Copyright 2012 SnowPlow Analytics Ltd
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

class GoogleReferalTest extends Specification {

  // From the README
  val refererUrl = "http://www.google.com/search?q=gateway+oracle+cards+denise+linn&hl=en&client=safari"
  val expected = Referer(Medium.Search, "Google", Some("gateway oracle cards denise linn"))

  "A Google referal" should {

    val referer = Parser.parse(refererUrl).get
    "have referer medium to \"%s\"".format(expected.medium) in {
      referer.medium must_== expected.medium
    }
    "have referer source to \"%s\"".format(expected.source) in {
      referer.source must_== expected.source
    }
    "have search term set to \"%s\"".format(expected.term.get) in {
      referer.term must_== expected.term
    }
  }
}