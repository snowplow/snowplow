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

import org.specs2.Specification
import org.specs2.matcher.DataTables

class ValidateAndReformatJsonSpec extends Specification with DataTables {
  def is = s2"""
  extracting and reformatting (where necessary) valid JSONs with work $e1
  extracting invalid JSONs should fail $e2
  """

  def e1 =
    "SPEC NAME" || "INPUT STR" | "EXPECTED" |
      "Empty JSON" !! "{}" ! "{}" |
      "Simple JSON #1" !! """{"key":"value"}""" ! """{"key":"value"}""" |
      "Simple JSON #2" !! """[1,2,3]""" ! """[1,2,3]""" |
      "Reformatted JSON #1" !! """{ "key" : 23 }""" ! """{"key":23}""" |
      "Reformatted JSON #2" !! """[1.00, 2.00, 3.00, 4.00]""" ! """[1.00,2.00,3.00,4.00]""" |
      "Reformatted JSON #3" !! """
      {
        "a": 23
      }""" ! """{"a":23}""" |> { (_, str, expected) =>
      JsonUtils.validateAndReformatJson(str) must beRight(expected)
    }

  def err1 = s"invalid json: exhausted input"
  def err2: (String, Int, Int) => String =
    (got, line, col) => s"invalid json: expected json value got '$got' (line $line, column $col)"
  def err3: (String, Int, Int) => String =
    (got, line, col) => s"""invalid json: expected " got '$got' (line $line, column $col)"""

  def e2 =
    "SPEC NAME" || "INPUT STR" | "EXPECTED" |
      "Empty string" !! "" ! err1 |
      "Double colons" !! """{"a"::2}""" ! err2(":2}", 1, 6) |
      "Random noise" !! "^45fj_" ! err2("^45fj_", 1, 1) |
      "Bad key" !! """{9:"a"}""" ! err3("""9:"a"}""", 1, 2) |> { (_, str, expected) =>
      JsonUtils.validateAndReformatJson(str) must beLeft(expected)
    }

}
