/*
 * Copyright (c) 2015 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.enrich.common.loaders

import org.specs2.mutable.Specification
import org.specs2.scalaz.ValidationMatchers

import scalaz._
import Scalaz._

class NdjsonLoaderSpec extends Specification with ValidationMatchers {

  "toCollectorPayload" should {

    "return failure on unparsable json" in {
      val invalid = NdjsonLoader("com.abc/v1").toCollectorPayload("""{ ... """)
      invalid must beFailing
    }

    "return success on parsable json" in {
      val valid = NdjsonLoader("com.abc/v1").toCollectorPayload("""{ "key": "value" } """")
      valid must beSuccessful
    }

    "return success with no content for empty rows" in {
      NdjsonLoader("com.abc/v1").toCollectorPayload("\r\n") must beSuccessful(None)
    }

    "fail if multiple lines passed in as one line" in {
      val lines = List("""{"key":"value1"}""", """{"key":"value2"}""")
      NdjsonLoader("com.abc/v1").toCollectorPayload(lines.mkString("\n")) must beFailing(
        NonEmptyList("Too many lines! Expected single line"))
    }

  }

}
