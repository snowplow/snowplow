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
package com.snowplowanalytics.snowplow.enrich.common
package enrichments.registry
package apirequest

import cats.Eval
import cats.data.ValidatedNel
import cats.syntax.option._

import io.circe.literal._

import com.snowplowanalytics.iglu.core.{SchemaCriterion, SchemaKey, SchemaVer, SelfDescribingData}

import org.specs2.Specification
import org.specs2.matcher.ValidatedMatchers
import outputs.EnrichedEvent

class InputSpec extends Specification with ValidatedMatchers {
  def is = s2"""
  create template context from POJO inputs              $e1
  create template context from JSON inputs              $e2
  colliding inputs from JSON and POJO didn't get merged $e3
  collect failures                                      $e4
  POJO null value return successful None                $e5
  POJO invalid key return failure                       $e6
  skip lookup on missing (in event) key                 $e7
  match modelless criterion (*-*-*)                     $e8
  """

  object ContextCase {
    val ccInput = Input.Json(
      "nullableValue",
      "contexts",
      SchemaCriterion("org.ietf", "http_cookie", "jsonschema", 1),
      "$.value"
    )
    val derInput = Input.Json(
      "overridenValue",
      "derived_contexts",
      SchemaCriterion("org.openweathermap", "weather", "jsonschema", 1, 0),
      "$.main.humidity"
    )
    val unstructInput = Input.Json(
      "unstructValue",
      "unstruct_event",
      SchemaCriterion(
        "com.snowplowanalytics.monitoring.batch",
        "jobflow_step_status",
        "jsonschema",
        1,
        0,
        0
      ),
      "$.state"
    )
    val overrideHumidityInput = Input.Json(
      "overridenValue",
      "contexts",
      SchemaCriterion(
        "com.snowplowanalytics.snowplow",
        "geolocation_context",
        "jsonschema",
        1,
        1
      ),
      "$.latitude"
    )

    val derivedContext1 =
      SelfDescribingData(
        SchemaKey("org.openweathermap", "weather", "jsonschema", SchemaVer.Full(1, 0, 0)),
        json"""{
          "clouds": {
              "all": 0
          },
          "dt": "2014-11-10T08:38:30.000Z",
          "main": {
              "grnd_level": 1021.91,
              "humidity": 90,
              "pressure": 1021.91,
              "sea_level": 1024.77,
              "temp": 301.308,
              "temp_max": 301.308,
              "temp_min": 301.308
          },
          "weather": [ { "description": "Sky is Clear", "icon": "01d", "id": 800, "main": "Clear" } ],
          "wind": {
              "deg": 190.002,
              "speed": 4.39
          }
        }"""
      )

    val cookieContext =
      SelfDescribingData(
        SchemaKey("org.ietf", "http_cookie", "jsonschema", SchemaVer.Full(1, 0, 0)),
        json"""{"name": "someCookieAgain", "value": null}"""
      )

    val unstructEvent =
      SelfDescribingData(
        SchemaKey(
          "com.snowplowanalytics.monitoring.batch",
          "jobflow_step_status",
          "jsonschema",
          SchemaVer.Full(1, 0, 0)
        ),
        json"""{"name": "Some EMR Job", "state": "COMPLETED"}"""
      )

    val overriderContext =
      SelfDescribingData(
        SchemaKey(
          "com.snowplowanalytics.snowplow",
          "geolocation_context",
          "jsonschema",
          SchemaVer.Full(1, 1, 0)
        ),
        json"""{"latitude": 43.1, "longitude": 32.1}"""
      )
  }

  def e1 = {
    val input1 = Input.Pojo("user", "user_id")
    val input2 = Input.Pojo("time", "true_tstamp")
    val event = new EnrichedEvent
    event.setUser_id("chuwy")
    event.setTrue_tstamp("20")
    val templateContext = Input.buildTemplateContext(List(input1, input2), event, Nil, Nil, None)
    templateContext must beValid(Some(Map("user" -> "chuwy", "time" -> "20")))
  }

  def e2 = {
    import ContextCase._
    val event = new EnrichedEvent
    val templateContext = Input.buildTemplateContext(
      List(ccInput, derInput, unstructInput, overrideHumidityInput),
      event,
      derivedContexts = List(derivedContext1),
      customContexts = List(cookieContext, overriderContext),
      unstructEvent = Some(unstructEvent)
    )
    templateContext must beValid(
      Some(
        Map("nullableValue" -> "null", "overridenValue" -> "43.1", "unstructValue" -> "COMPLETED")
      )
    )
  }

  def e3 = {
    val jsonLatitudeInput = Input.Json(
      "latitude",
      "contexts",
      SchemaCriterion(
        "com.snowplowanalytics.snowplow",
        "geolocation_context",
        "jsonschema",
        1,
        1
      ),
      "$.latitude"
    )
    val pojoLatitudeInput = Input.Pojo("latitude", "geo_latitude")
    val event = new EnrichedEvent
    event.setGeo_latitude(42.0f)

    val templateContext = Input.buildTemplateContext(
      List(pojoLatitudeInput, jsonLatitudeInput),
      event,
      derivedContexts = Nil,
      customContexts = List(ContextCase.overriderContext),
      unstructEvent = None
    )

    templateContext must beValid(Some(Map("latitude" -> "43.1")))
  }

  def e4 = {
    val invalidJsonPathInput = Input.Json(
      "latitude",
      "contexts",
      SchemaCriterion(
        "com.snowplowanalytics.snowplow",
        "geolocation_context",
        "jsonschema",
        1,
        1
      ),
      "*.invalidJsonPath"
    )
    val invalidJsonFieldInput = Input.Json(
      "latitude",
      "invalid_field",
      SchemaCriterion(
        "com.snowplowanalytics.snowplow",
        "geolocation_context",
        "jsonschema",
        1,
        1
      ),
      "$.validJsonPath"
    )
    val pojoInput = Input.Pojo("latitude", "app_id")

    val templateContext = Input.buildTemplateContext(
      List(invalidJsonPathInput, pojoInput, invalidJsonFieldInput),
      null,
      derivedContexts = Nil,
      customContexts = Nil, // This should have been an error?
      unstructEvent = None
    )
    templateContext must beInvalid.like {
      case errors => errors.toList must have length 3
    }
  }

  def e5 = {
    val event = new EnrichedEvent
    val pojoInput = Input.Pojo("someKey", "app_id")
    val templateContext: ValidatedNel[String, Option[Any]] = pojoInput.pull(event, Nil, Nil, None)
    templateContext must beValid.like {
      case map => map must beNone
    }
  }

  def e6 = {
    val event = new EnrichedEvent
    val pojoInput = Input.Pojo("someKey", "unknown_property")
    val templateContext: ValidatedNel[String, Option[Any]] = pojoInput.pull(event, Nil, Nil, None)
    templateContext must beInvalid
  }

  def e7 = {
    val input1 = Input.Pojo("user", "user_id")
    val input2 = Input.Pojo("time", "true_tstamp")
    val uriTemplate = "http://thishostdoesntexist31337:8123/{{  user }}/foo/{{ time}}/{{user}}"
    val schemaKey = SchemaKey("vendor", "name", "format", SchemaVer.Full(1, 0, 0))
    val enrichment = ApiRequestConf(
      schemaKey,
      List(input1, input2),
      HttpApi("GET", uriTemplate, 1000, Authentication(None)),
      List(Output("iglu:someschema", JsonOutput("$").some)),
      Cache(10, 5)
    ).enrichment[Eval]
    val event = new outputs.EnrichedEvent
    event.setUser_id("chuwy")
    // time in true_tstamp won't be found
    val request = enrichment.value.lookup(event, Nil, Nil, None).value
    request must beValid.like {
      case response => response must be(Nil)
    }
  }

  def e8 = {
    val input = Input.Json(
      "permissive",
      "contexts",
      SchemaCriterion("com.snowplowanalytics", "some_schema", "jsonschema"),
      "$.somekey"
    )

    val obj =
      SelfDescribingData(
        SchemaKey("com.snowplowanalytics", "some_schema", "jsonschema", SchemaVer.Full(2, 0, 1)),
        json"""{ "somekey": "somevalue" }"""
      )

    input.pull(new outputs.EnrichedEvent, Nil, List(obj), None) must beValid.like {
      case Some(context) =>
        context must beEqualTo(Map("permissive" -> "somevalue"))
      case None =>
        ko("Context is missing")
    }
  }
}
