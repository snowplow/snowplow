/*
 * Copyright (c) 2012-2020 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
 */
package com.snowplowanalytics.snowplow.enrich.common.utils

import io.circe._
import io.circe.syntax._
import org.specs2.Specification

class JsonPathSpec extends Specification {
  def is = s2"""
  test JSONPath query                     $e1
  test query of non-exist value           $e2
  test query of empty array               $e3
  test primtive JSON type (JString)       $e6
  invalid JSONPath (JQ syntax) must fail  $e4
  invalid JSONPath must fail              $e5
  """

  val someJson = Json.obj(
    "store" := Json.obj(
      "book" := Json.fromValues(
        List(
          Json.obj(
            "category" := Json.fromString("reference"),
            "author" := Json.fromString("Nigel Rees"),
            "title" := Json.fromString("Savings of the Century"),
            "price" := Json.fromDoubleOrNull(8.95)
          ),
          Json.obj(
            "category" := Json.fromString("fiction"),
            "author" := Json.fromString("Evelyn Waugh"),
            "title" := Json.fromString("Swords of Honour"),
            "price" := Json.fromDoubleOrNull(12.99)
          ),
          Json.obj(
            "category" := Json.fromString("fiction"),
            "author" := Json.fromString("Herman Melville"),
            "title" := Json.fromString("Moby Dick"),
            "isbn" := Json.fromString("0-553-21311-3"),
            "price" := Json.fromDoubleOrNull(8.99)
          ),
          Json.obj(
            "category" := Json.fromString("fiction"),
            "author" := Json.fromString("J. R. R. Tolkien"),
            "title" := Json.fromString("The Lord of the Rings"),
            "isbn" := Json.fromString("0-395-19395-8"),
            "price" := Json.fromDoubleOrNull(22.99)
          )
        )
      ),
      "bicycles" := Json.obj(
        "color" := Json.fromString("red"),
        "price" := Json.fromDoubleOrNull(19.95)
      ),
      "unicors" := Json.fromValues(Nil)
    )
  )

  def e1 =
    JsonPath.query("$.store.book[1].price", someJson) must
      beRight(List(Json.fromDoubleOrNull(12.99)))

  def e2 =
    JsonPath.query("$.store.book[5].price", someJson) must beRight(Nil)

  def e3 =
    JsonPath.query("$.store.unicorns", someJson) must beRight(Nil)

  def e4 =
    JsonPath.query(".notJsonPath", someJson) must beLeft.like {
      case f => f must beEqualTo("'$' expected but '.' found")
    }

  def e5 =
    JsonPath.query("$.store.book[a]", someJson) must beLeft.like {
      case f => f must beEqualTo("':' expected but 'a' found")
    }

  def e6 =
    JsonPath.query("$.store.book[2]", Json.fromString("somestring")) must beRight(List())
}
