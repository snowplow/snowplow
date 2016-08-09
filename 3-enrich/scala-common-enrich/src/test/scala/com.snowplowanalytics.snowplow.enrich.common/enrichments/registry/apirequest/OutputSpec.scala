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
package com.snowplowanalytics.snowplow.enrich.common
package enrichments.registry
package apirequest

// specs2
import org.specs2.Specification
import org.specs2.scalaz.ValidationMatchers

// json4s
import org.json4s.JObject
import org.json4s.JsonDSL._

class OutputSpec extends Specification with ValidationMatchers  { def is =
  "This is a specification to test the HTTP API of API Request Enrichment" ^
                                                                          p^
    "Not found value result in Failure"                                ! e1^
    "Successfully generate context"                                    ! e2^
    "Successfully generate context out of complex object"              ! e3^
                                                                       end

  def e1 = {
    val output = Output("iglu:com.snowplowanalytics/some_schema/jsonschema/1-0-0", Some(JsonOutput("$.value")))
    output.extract(JObject(Nil)) must beFailing
  }

  def e2 = {
    val output = Output("iglu:com.snowplowanalytics/some_schema/jsonschema/1-0-0", Some(JsonOutput("$.value")))
    output.parse("""{"value": 32}""").flatMap(output.extract).map(output.describeJson) must beSuccessful.like {
      case context => context must be equalTo(("schema", "iglu:com.snowplowanalytics/some_schema/jsonschema/1-0-0") ~ ("data" -> 32))
    }
  }

  def e3 = {
    val output = Output("iglu:com.snowplowanalytics/complex_schema/jsonschema/1-0-0", Some(JsonOutput("$.objects[1].deepNesting[3]")))
    output.parse(
      """
        |{
        |  "value": 32,
        |  "objects":
        |  [
        |    {"wrongValue": 11},
        |    {"deepNesting": [1,2,3,42]},
        |    {"wrongValue": 10}
        |  ]
        |}
      """.stripMargin).flatMap(output.extract).map(output.describeJson) must beSuccessful.like {
      case context => context must be equalTo(("schema", "iglu:com.snowplowanalytics/complex_schema/jsonschema/1-0-0") ~ ("data" -> 42))

    }

  }
}
