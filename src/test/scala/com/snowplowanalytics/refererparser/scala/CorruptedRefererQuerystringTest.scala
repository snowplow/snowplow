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

// Java
import java.net.URI

// Specs2
import org.specs2.mutable.Specification

class CorruptedRefererQuerystringTest extends Specification {

  // Our data
  val refererUri = "http://www.google.com/search?q=Psychic+Bazaar&sugexp=chrome,mod=3&sourceid=chrome&ie=UTF-8"
  val expected   = Some(Referer(Medium.Search, Some("Google"), Some("Psychic Bazaar")))

  "A corrupted referer querystring" should {
    "identify the search engine but not the search term" in {
      Parser.parse(refererUri, "") must_== expected
    }
  }
}