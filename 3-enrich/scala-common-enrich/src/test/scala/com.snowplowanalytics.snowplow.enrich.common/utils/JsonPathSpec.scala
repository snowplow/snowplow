/*
 * Copyright (c) 2012-2016 Snowplow Analytics Ltd. All rights reserved.
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

// specs2
import org.specs2.Specification
import org.specs2.scalaz.ValidationMatchers

// json4s
import org.json4s._
import org.json4s.jackson.parseJson

class JsonPathSpec extends Specification with ValidationMatchers { def is =
  "This is a specification to test the JSONPath utils"                     ^
    "Test JSONPath query"                                             ! e1 ^
    "Test query of non-exist value"                                   ! e2 ^
    "Test query of empty array"                                       ! e3 ^
    "Test primtive JSON type (JString)"                               ! e6 ^
    "Invalid JSONPath (JQ syntax) must fail"                          ! e4 ^
    "Invalid JSONPath must fail"                                      ! e5 ^
    "JNothing must fail"                                              ! e7 ^
                                                                      end

  val someJson = parseJson("""
      |{ "store": {
      |    "book": [
      |      { "category": "reference",
      |        "author": "Nigel Rees",
      |        "title": "Sayings of the Century",
      |        "price": 8.95
      |      },
      |      { "category": "fiction",
      |        "author": "Evelyn Waugh",
      |        "title": "Sword of Honour",
      |        "price": 12.99
      |      },
      |      { "category": "fiction",
      |        "author": "Herman Melville",
      |        "title": "Moby Dick",
      |        "isbn": "0-553-21311-3",
      |        "price": 8.99
      |      },
      |      { "category": "fiction",
      |        "author": "J. R. R. Tolkien",
      |        "title": "The Lord of the Rings",
      |        "isbn": "0-395-19395-8",
      |        "price": 22.99
      |      }
      |    ],
      |    "bicycle": {
      |      "color": "red",
      |      "price": 19.95
      |    },
      |    "unicorns": []
      |  }
      |}
    """.stripMargin)


  def e1 =
    JsonPath.query("$.store.book[1].price", someJson) must beSuccessful(List(JDouble(12.99)))

  def e2 =
    JsonPath.query("$.store.book[5].price", someJson) must beSuccessful(Nil)

  def e3 =
    JsonPath.query("$.store.unicorns", someJson) must beSuccessful(List(JArray(Nil)))

  def e4 =
    JsonPath.query(".notJsonPath", someJson) must beFailing.like {
      case f => f must beEqualTo("`$' expected but `.' found")
    }

  def e5 =
    JsonPath.query("$.store.book[a]", someJson) must beFailing.like {
      case f => f must beEqualTo("`:' expected but `a' found")
    }

  def e6 =
    JsonPath.query("$.store.book[2]", JString("somestring")) must beSuccessful(List())

  def e7 =
    JsonPath.query("$..", JNothing) must beFailing.like {
      case f => f must beEqualTo("JSONPath error: Nothing was given")
    }
}
