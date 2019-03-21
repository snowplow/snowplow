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
package com.snowplowanalytics.snowplow.enrich.common
package enrichments.registry.apirequest

import cats.data.ValidatedNel
import cats.syntax.option._
import io.circe._
import io.circe.literal._
import org.specs2.Specification
import org.specs2.matcher.ValidatedMatchers

import outputs.EnrichedEvent

class InputSpec extends Specification with ValidatedMatchers {
  def is = s2"""
  This is a specification to test the Inputs and template context building of API Request Enrichment
  Create template context from POJO inputs              $e1
  Create template context from JSON inputs              $e2
  Colliding inputs from JSON and POJO didn't get merged $e3
  Collect failures                                      $e4
  POJO null value return successful None                $e5
  POJO invalid key return failure                       $e6
  Skip lookup on missing (in event) key                 $e7
  Match modelless criterion (*-*-*)                     $e8
  """

  object ContextCase {
    val ccInput = Input(
      "nullableValue",
      pojo = None,
      json = JsonInput("contexts", "iglu:org.ietf/http_cookie/jsonschema/1-*-*", "$.value").some
    )
    val derInput = Input(
      "overridenValue",
      pojo = None,
      json = JsonInput(
        "derived_contexts",
        "iglu:org.openweathermap/weather/jsonschema/1-0-*",
        "$.main.humidity"
      ).some
    )
    val unstructInput = Input(
      "unstructValue",
      pojo = None,
      json = JsonInput(
        "unstruct_event",
        "iglu:com.snowplowanalytics.monitoring.batch/jobflow_step_status/jsonschema/1-0-0",
        "$.state"
      ).some
    )
    val overrideHumidityInput = Input(
      "overridenValue",
      pojo = None,
      json = JsonInput(
        "contexts",
        "iglu:com.snowplowanalytics.snowplow/geolocation_context/jsonschema/1-1-*",
        "$.latitude"
      ).some
    )

    val derivedContext1 = json"""{
      "schema": "iglu:org.openweathermap/weather/jsonschema/1-0-0",
      "data": {
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
      }
    }"""

    val cookieContext = json"""{
      "schema": "iglu:org.ietf/http_cookie/jsonschema/1-0-0",
      "data": {"name": "someCookieAgain", "value": null}
    }"""

    val unstructEvent = json"""{
      "schema": "iglu:com.snowplowanalytics.monitoring.batch/jobflow_step_status/jsonschema/1-0-0",
      "data": {"name": "Some EMR Job", "state": "COMPLETED"}
    }"""

    val overriderContext = json"""{
      "schema": "iglu:com.snowplowanalytics.snowplow/geolocation_context/jsonschema/1-1-0",
      "data": {"latitude": 43.1, "longitude": 32.1}
    }"""
  }

  def e1 = {
    val input1 = Input("user", pojo = PojoInput("user_id").some, json = None)
    val input2 = Input("time", pojo = PojoInput("true_tstamp").some, json = None)
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
        Map("nullableValue" -> "null", "overridenValue" -> "43.1", "unstructValue" -> "COMPLETED")))
  }

  def e3 = {
    val jsonLatitudeInput = Input(
      "latitude",
      pojo = None,
      json = JsonInput(
        "contexts",
        "iglu:com.snowplowanalytics.snowplow/geolocation_context/jsonschema/1-1-*",
        "$.latitude").some)
    val pojoLatitudeInput = Input("latitude", pojo = PojoInput("geo_latitude").some, json = None)
    val jsonLatitudeContext = json"""{
      "schema": "iglu:com.snowplowanalytics.snowplow/geolocation_context/jsonschema/1-1-0",
      "data": {"latitude": 43.1, "longitude": 32.1}
    }"""
    val event = new EnrichedEvent
    event.setGeo_latitude(42.0f)

    val templateContext = Input.buildTemplateContext(
      List(jsonLatitudeInput, pojoLatitudeInput),
      event,
      derivedContexts = Nil,
      customContexts = List(jsonLatitudeContext),
      unstructEvent = None)
    templateContext must beValid(Some(Map("latitude" -> "43.1")))
  }

  def e4 = {
    val invalidJsonPathInput = Input(
      "latitude",
      pojo = None,
      json = JsonInput(
        "contexts",
        "iglu:com.snowplowanalytics.snowplow/geolocation_context/jsonschema/1-1-*",
        "*.invalidJsonPath").some)
    val invalidJsonFieldInput = Input(
      "latitude",
      pojo = None,
      json = JsonInput(
        "invalid_field",
        "iglu:com.snowplowanalytics.snowplow/geolocation_context/jsonschema/1-1-*",
        "$.validJsonPath").some
    )
    val pojoInput = Input("latitude", pojo = PojoInput("app_id").some, json = None)

    val templateContext = Input.buildTemplateContext(
      List(invalidJsonPathInput, pojoInput, invalidJsonFieldInput),
      null,
      derivedContexts = Nil,
      customContexts = List(Json.fromJsonObject(JsonObject.empty)),
      unstructEvent = None
    )
    templateContext must beInvalid.like {
      case errors => errors.toList must have length (3)
    }
  }

  def e5 = {
    val event = new EnrichedEvent
    val pojoInput = Input("someKey", pojo = PojoInput("app_id").some, json = None)
    val templateContext: ValidatedNel[String, Option[Any]] = pojoInput.getFromEvent(event)
    templateContext must beValid.like {
      case map => map must beNone
    }
  }

  def e6 = {
    val event = new EnrichedEvent
    val pojoInput = Input("someKey", pojo = PojoInput("unknown_property").some, json = None)
    val templateContext: ValidatedNel[String, Option[Any]] = pojoInput.getFromEvent(event)
    templateContext must beInvalid
  }

  def e7 = {
    val input1 = Input("user", Some(PojoInput("user_id")), None)
    val input2 = Input("time", Some(PojoInput("true_tstamp")), None)
    val uriTemplate = "http://thishostdoesntexist31337:8123/{{  user }}/foo/{{ time}}/{{user}}"
    val enrichment = ApiRequestEnrichment(
      List(input1, input2),
      HttpApi("GET", uriTemplate, 1000, Authentication(None)),
      List(Output("iglu:someschema", JsonOutput("$").some)),
      Cache(10, 5))
    val event = new outputs.EnrichedEvent
    event.setUser_id("chuwy")
    // time in true_tstamp won't be found
    val request = enrichment.lookup(event, Nil, Nil, Nil)
    request must beValid.like {
      case response => response must be(Nil)
    }
  }

  def e8 = {
    val input = Input(
      "permissive",
      None,
      Some(
        JsonInput(
          "contexts",
          "iglu:com.snowplowanalytics/some_schema/jsonschema/*-*-*",
          "$.somekey"))
    )

    val obj = json"""{
      "schema": "iglu:com.snowplowanalytics/some_schema/jsonschema/2-0-1",
      "data": { "somekey": "somevalue" }
    }"""

    input.getFromJson(Nil, List(obj), None) must beValid.like {
      case option =>
        option must beSome.like {
          case context => context must beEqualTo(Map("permissive" -> "somevalue"))
        }
    }
  }
}
