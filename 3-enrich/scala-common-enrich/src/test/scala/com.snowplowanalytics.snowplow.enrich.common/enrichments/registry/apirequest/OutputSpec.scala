/*
 * Copyright (c) 2012-2019 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.enrich.common.enrichments.registry.apirequest

import io.circe._
import io.circe.literal._
import org.specs2.Specification

class OutputSpec extends Specification {
  def is = s2"""
  Not found value result in Failure                   $e1
  Successfully generate context                       $e2
  Successfully generate context out of complex object $e3
  """

  def e1 = {
    val output =
      Output("iglu:com.snowplowanalytics/some_schema/jsonschema/1-0-0", Some(JsonOutput("$.value")))
    output.extract(Json.fromJsonObject(JsonObject.empty)) must beLeft
  }

  def e2 = {
    val output = Output(
      "iglu:com.snowplowanalytics/some_schema/jsonschema/1-0-0",
      Some(JsonOutput("$.value"))
    )
    output
      .parseResponse("""{"value": 32}""")
      .flatMap(output.extract)
      .map(output.describeJson) must beRight.like {
      case context =>
        context must be equalTo json"""{
            "schema": "iglu:com.snowplowanalytics/some_schema/jsonschema/1-0-0",
            "data": 32
          }"""
    }
  }

  def e3 = {
    val output = Output(
      "iglu:com.snowplowanalytics/complex_schema/jsonschema/1-0-0",
      Some(JsonOutput("$.objects[1].deepNesting[3]"))
    )
    output.parseResponse("""
        |{
        |  "value": 32,
        |  "objects":
        |  [
        |    {"wrongValue": 11},
        |    {"deepNesting": [1,2,3,42]},
        |    {"wrongValue": 10}
        |  ]
        |}
      """.stripMargin).flatMap(output.extract).map(output.describeJson) must beRight.like {
      case context =>
        context must be equalTo json"""{
          "schema": "iglu:com.snowplowanalytics/complex_schema/jsonschema/1-0-0",
          "data": 42
        }"""
    }

  }
}
