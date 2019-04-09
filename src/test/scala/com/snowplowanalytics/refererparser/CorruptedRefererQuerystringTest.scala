/**
 * Copyright 2012-2019 Snowplow Analytics Ltd
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
package com.snowplowanalytics.refererparser

import cats.Eval
import cats.effect.IO
import org.specs2.mutable.Specification

class CorruptedRefererQuerystringTest extends Specification {

  val resource   = getClass.getResource("/referers.json").getPath
  val ioParser   = CreateParser[IO].create(resource).unsafeRunSync().fold(throw _, identity)
  val evalParser = CreateParser[Eval].create(resource).value.fold(throw _, identity)

  "A corrupted referer querystring" should {
    "identify the search engine but not the search term" in {
      val refererUri =
        "http://www.google.com/search?q=Psychic+Bazaar&sugexp=chrome,mod=3&sourceid=chrome&ie=UTF-8"
      val expected = Some(SearchReferer(SearchMedium, "Google", Some("Psychic Bazaar")))
      ioParser.parse(refererUri, "") must_== expected
      evalParser.parse(refererUri, "") must_== expected
    }
  }
}
