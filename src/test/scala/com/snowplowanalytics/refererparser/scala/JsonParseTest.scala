/**
 * Copyright 2014 Snowplow Analytics Ltd
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

// json4s
import org.json4s._
import org.json4s.jackson.JsonMethods._

// Specs2
import org.specs2.mutable.Specification

class JsonParseTest extends Specification {

  val testString = fromFile("src/test/resources/referer-tests.json").getLines.mkString

  // Convert the JSON to a List of JObjects
  val testJson = (parse(testString)) match {
    case JArray(lst) => lst
    case _ => throw new Exception("referer-tests.json is not an array - this should never happen")
  }

  val pageHost = "www.snowplowanalytics.com"

  val internalDomains = List("www.subdomain1.snowplowanalytics.com", "www.subdomain2.snowplowanalytics.com")

  def getString(node: JValue, name: String): String = 
    (node \ name) match {
      case JString(s) => s
      case _ => throw new Exception("The value of field '%s' in referer-tests.json is not a string - this should never happen".format(name))
  }

  "parse" should {

    for (test <- testJson) {

      "extract the expected details from referer with spec '%s'".format(getString(test, "spec")) in {

        Parser.parse(getString(test, "uri"), pageHost, internalDomains) must_== 
          Some(Referer(
            Medium.withName(getString(test, "medium")),
            (test \ "source") match {
              case JString(s) => Some(s)
              case _ => None
            },
            (test \ "term") match {
              case JString(s) => Some(s)
              case _ => None
            }))
      }
    }
  }

}
