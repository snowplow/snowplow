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

// Java
import java.net.URI

// Specs2
import org.specs2.mutable.Specification

// Cats
import cats.effect.IO

class NoPageUriTest extends Specification {

  // Our data
  val refererUri = "http://www.google.com/search?q=gateway+oracle+cards+denise+linn&hl=en&client=safari"
  val expected   = Some(Referer(Medium.Search, Some("Google"), Some("gateway oracle cards denise linn")))

  val parser = Parser.create[IO](
    getClass.getResource("/referers.json").getPath
  ).unsafeRunSync() match {
    case Right(p) => p
    case Left(f) => throw f
  }

  "An empty page URI" should {
    "not interfere with the referer parsing" in {
      parser.parse(refererUri, "") must_== expected
    }
  }

  "No page URI" should {
    "not interfere with the referer parsing" in {
      parser.parse(refererUri) must_== expected
    }
  }

  "A page URI" should {
    "not interfere with the referer parsing" in {
      parser.parse(refererUri) must_== expected
    }
  }
}
