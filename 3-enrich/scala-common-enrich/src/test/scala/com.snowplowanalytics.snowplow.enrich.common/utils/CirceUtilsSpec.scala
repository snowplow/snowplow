/*
 * Copyright (c) 2014-2019 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.enrich.common
package utils

import io.circe.literal._
import org.specs2.matcher.ValidatedMatchers
import org.specs2.mutable.Specification

class CirceUtilsSpec extends Specification with ValidatedMatchers {
  val testJson = json"""{
    "outer": "1",
    "inner": {
      "value": 2
      }
    }"""

  "Applying extractString" should {
    "successfully access an outer string field" in {
      val result = CirceUtils.extract[String](testJson, "outer")
      result must beValid("1")
    }
  }

  "Applying extractInt" should {
    "successfully access an inner string field" in {
      val result = CirceUtils.extract[Int](testJson, "inner", "value")
      result must beValid(2)
    }
  }

}
