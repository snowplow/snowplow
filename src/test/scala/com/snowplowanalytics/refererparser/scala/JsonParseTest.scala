/**
 * Copyright 2018 Snowplow Analytics Ltd
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

// Scala
import scala.io.Source._

// circe
import io.circe._;
import io.circe.parser._;
import io.circe.syntax._;
import io.circe.generic.semiauto._;

// Specs2
import org.specs2.mutable.Specification

// cats
import cats.effect.IO

case class TestCase(
  spec: String,
  uri: String,
  medium: String,
  source: Option[String],
  term: Option[String],
  known: Boolean
)

class JsonParseTest extends Specification {
  implicit val testCaseDecoder: Decoder[TestCase] = deriveDecoder[TestCase]

  val testString = fromFile("src/test/resources/referer-tests.json").getLines.mkString

  // Convert the JSON to a List of TestCase
  val eitherTests = for {
    doc <- parse(testString)
    lst <- doc.as[List[Json]]
  } yield lst.map(_.as[TestCase] match {
    case Right(success) => success
    case Left(failure) => throw failure
  })

  val tests = eitherTests match {
    case Right(success) => success
    case Left(failure) => throw failure
  }

  val pageHost = "www.snowplowanalytics.com"

  val internalDomains = List("www.subdomain1.snowplowanalytics.com", "www.subdomain2.snowplowanalytics.com")

  "parse" should {
    "extract the expected details from referer with spec" in {
      for (test <- tests) yield {
        Parser.parse[IO](test.uri, pageHost, internalDomains).unsafeRunSync() shouldEqual
          Some(Referer(
            Medium.withName(test.medium),
            test.source,
            test.term
          ))
      }
    }
  }

}
