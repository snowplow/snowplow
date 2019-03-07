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

import cats.effect.IO
import org.specs2.{Specification, ScalaCheck}
import org.scalacheck.Arbitrary._

class ParseFuzzTest extends Specification with ScalaCheck {

  val parser = Parser.create[IO](
    getClass.getResource("/referers.json").getPath
  ).unsafeRunSync() match {
    case Right(p) => p
    case Left(f) => throw f
  }

  def is =
    "The parse function should work for any pair of referer and page Strings" ! e1

  def e1 =
    prop { (refererUri: String, pageUri: String) =>
      parser.parse(refererUri, pageUri) must beAnInstanceOf[Option[Referer]]
    }
}
