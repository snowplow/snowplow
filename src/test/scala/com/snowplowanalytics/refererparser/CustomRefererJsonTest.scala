/**
 * Copyright 2012-2018 Snowplow Analytics Ltd
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

import scala.io.Source

// Java
import java.net.URI

// Specs2
import org.specs2.mutable.Specification

// Cats
import cats.effect.IO

class CustomRefererJsonTest extends Specification {

  "Custom referer list" should {
    "give correct referer" in {
      val customReferersPath = getClass.getResource("/custom-referers.json").getPath

      val parser = Parser.create[IO](customReferersPath).unsafeRunSync() match {
        case Right(parser) => parser
        case Left(failure) => throw failure
      }
      val expected = Some(Referer(Medium.Search, Some("Example"), Some("hello world")))
      val actual = parser.parse("https://www.example.org/?query=hello+world")

      expected shouldEqual actual
    }
  }
}
