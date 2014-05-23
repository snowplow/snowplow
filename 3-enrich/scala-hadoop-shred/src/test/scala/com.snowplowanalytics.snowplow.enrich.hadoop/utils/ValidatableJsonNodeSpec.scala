/*
 * Copyright (c) 2012-2014 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.enrich
package hadoop
package utils

// Jackson
import com.fasterxml.jackson.databind.{
  ObjectMapper,
  JsonNode
}
import com.github.fge.jackson.JsonLoader

// Scalding
import com.twitter.scalding.Args

// Scalaz
import scalaz._
import Scalaz._

// Specs2
import org.specs2.Specification
import org.specs2.matcher.DataTables
import org.specs2.scalaz.ValidationMatchers

object ValidatableJsonNodeSpec {

  private lazy val Mapper = new ObjectMapper
  
  def asJsonNode(str: String) =
    Mapper.readTree(str)
}

class ValidatableJsonNodeSpec extends Specification with DataTables with ValidationMatchers { def is =

  "This is a specification to test the ValidatableJsonNode functionality"  ^
                                                                          p^
  "a JsonNode should be pimped to a ValidatableJsonNode as needed"         ! e1^
  "JsonNodes that pass schema validation should be wrapped in a Success"   ! e2^  
                                                                           end

  val SimpleSchema = JsonLoader.fromResource("/jsonschema/simple_schema.json")

  import ValidatableJsonNode._
  import ValidatableJsonNodeSpec._

  def e1 = {
    val json = asJsonNode("""{"country": "JP", "beers": ["Asahi", "Orion", "..."]}""")
    json.validate(SimpleSchema) must beSuccessful(json)
  }

  def e2 =
    foreach(Seq(
      """{"country": "AQ", "beers": []}""",
      """{"country":"GR","beers":["Fix","Mythos"]}""",
      """{"country": "fr","beers": ["Jenlain"]}"""
    )) {
      str: String => {
        val json = asJsonNode(str)
        json.validate(SimpleSchema) must beSuccessful(json)
      }
    }
}
