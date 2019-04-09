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

class NoPageUriTest extends Specification {

  val refererUri =
    "http://www.google.com/search?q=gateway+oracle+cards+denise+linn&hl=en&client=safari"
  val expected = Some(
    SearchReferer(SearchMedium, "Google", Some("gateway oracle cards denise linn")))

  val resource   = getClass.getResource("/referers.json").getPath
  val ioParser   = CreateParser[IO].create(resource).unsafeRunSync().fold(throw _, identity)
  val evalParser = CreateParser[Eval].create(resource).value.fold(throw _, identity)

  "An empty page URI" should {
    "not interfere with the referer parsing" in {
      ioParser.parse(refererUri, "") must_== expected
      evalParser.parse(refererUri, "") must_== expected
    }
  }

  "No page URI" should {
    "not interfere with the referer parsing" in {
      ioParser.parse(refererUri) must_== expected
      evalParser.parse(refererUri) must_== expected
    }
  }

  "A page URI" should {
    "not interfere with the referer parsing" in {
      ioParser.parse(refererUri) must_== expected
      evalParser.parse(refererUri) must_== expected
    }
  }
}
