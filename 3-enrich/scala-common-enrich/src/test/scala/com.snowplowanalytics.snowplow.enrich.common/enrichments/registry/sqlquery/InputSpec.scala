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
package enrichments.registry.sqlquery

import scala.collection.immutable.IntMap

import cats.syntax.either._
import io.circe._
import io.circe.literal._
import io.circe.parser._
import io.circe.syntax._
import org.specs2.Specification
import org.specs2.scalaz.ValidationMatchers
import scalaz._
import Scalaz._

import outputs.EnrichedEvent
import Input._

class InputSpec extends Specification with ValidationMatchers {
  def is = s2"""
  This is a specification to test the Inputs and placeholder-map building of SQL Query Enrichment
  Create template context from POJO inputs                 $e1
  Create template context from JSON inputs                 $e8
  Encountered null in JSON means absent value              $e2
  Colliding inputs from JSON and POJO didn't get merged    $e3
  Collect failures                                         $e4
  POJO null value return successful None                   $e5
  POJO invalid key return failure                          $e6
  Extract correct path-dependent values from JSON          $e7
  Create Some empty IntMap for empty list of Inputs        $e9
  Check all EnrichedEvent properties can be handled        $e10
  Extract correct path-dependent values from EnrichedEvent $e11
  """

  object ContextCase {
    val ccInput =
      Input(
        1,
        pojo = None,
        json = JsonInput("contexts", "iglu:org.ietf/http_cookie/jsonschema/1-*-*", "$.value").some)
    val derInput = Input(
      2,
      pojo = None,
      json = JsonInput(
        "derived_contexts",
        "iglu:org.openweathermap/weather/jsonschema/1-0-*",
        "$.main.humidity").some)
    val unstructInput =
      Input(
        3,
        pojo = None,
        json = JsonInput(
          "unstruct_event",
          "iglu:com.snowplowanalytics.monitoring.batch/jobflow_step_status/jsonschema/1-0-0",
          "$.state").some)
    val overrideHumidityInput = Input(
      2,
      pojo = None,
      json = JsonInput(
        "contexts",
        "iglu:com.snowplowanalytics.snowplow/geolocation_context/jsonschema/1-1-*",
        "$.latitude").some)

    val derivedContext1 = json"""
      {
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

    val cookieContext = json"""
      {
        "schema": "iglu:org.ietf/http_cookie/jsonschema/1-0-0",
        "data": {"name": "someCookieAgain", "value": null}
      }"""

    val cookieContextWithoutNull = json"""
      {
        "schema": "iglu:org.ietf/http_cookie/jsonschema/1-0-0",
        "data": {"name": "someCookieAgain", "value": "someValue"}
      }"""

    val unstructEvent = json"""
      {
        "schema": "iglu:com.snowplowanalytics.monitoring.batch/jobflow_step_status/jsonschema/1-0-0",
        "data": {"name": "Some EMR Job", "state": "COMPLETED"}
      }"""

    val overriderContext = Json.obj(
      "schema" := "iglu:com.snowplowanalytics.snowplow/geolocation_context/jsonschema/1-1-0",
      "data" := Json.obj(
        "latitude" := Json.fromDoubleOrNull(43.1),
        "longitude" := Json.fromDoubleOrNull(32.1)
      )
    )
  }

  def e1 = {
    val input1 = Input(1, pojo = PojoInput("user_id").some, json = None)
    val input2 = Input(2, pojo = PojoInput("true_tstamp").some, json = None)
    val event = new EnrichedEvent
    event.setUser_id("chuwy")
    event.setTrue_tstamp("20")

    val placeholderMap = Input.buildPlaceholderMap(List(input1, input2), event, Nil, Nil, None)
    placeholderMap must beSuccessful(
      Some(IntMap(1 -> StringPlaceholder.Value("chuwy"), 2 -> StringPlaceholder.Value("20"))))
  }

  def e2 = {
    import ContextCase._
    val event = new EnrichedEvent
    val placeholderMap = Input.buildPlaceholderMap(
      List(ccInput, derInput, unstructInput, overrideHumidityInput),
      event,
      derivedContexts = List(derivedContext1),
      customContexts = List(cookieContext, overriderContext),
      unstructEvent = Some(unstructEvent)
    )
    placeholderMap must beSuccessful(None)
  }

  def e8 = {
    import ContextCase._
    val event = new EnrichedEvent
    val placeholderMap = Input.buildPlaceholderMap(
      List(ccInput, derInput, unstructInput, overrideHumidityInput),
      event,
      derivedContexts = List(derivedContext1),
      customContexts = List(cookieContextWithoutNull, overriderContext),
      unstructEvent = Some(unstructEvent)
    )

    utils.JsonPath.testMe

    placeholderMap must beSuccessful(
      Some(
        IntMap(
          1 -> StringPlaceholder.Value("someValue"),
          2 -> DoublePlaceholder.Value(43.1d),
          3 -> StringPlaceholder.Value("COMPLETED"))
      )
    )
  }

  def e3 = {
    import ContextCase._
    val jsonLatitudeInput = Input(
      1,
      pojo = None,
      json = JsonInput(
        "contexts",
        "iglu:com.snowplowanalytics.snowplow/geolocation_context/jsonschema/1-1-*",
        "$.latitude").some)
    val pojoLatitudeInput = Input(1, pojo = PojoInput("geo_latitude").some, json = None)
    val event = new EnrichedEvent
    event.setGeo_latitude(42.0f)

    // In API Enrichment this colliding wrong
    val templateContext = Input.buildPlaceholderMap(
      List(jsonLatitudeInput, pojoLatitudeInput),
      event,
      derivedContexts = Nil,
      customContexts = List(overriderContext),
      unstructEvent = None)
    templateContext must beSuccessful(Some(IntMap(1 -> DoublePlaceholder.Value(43.1))))
  }

  def e4 = {
    val invalidJsonPathInput = Input(
      1,
      pojo = None,
      json = JsonInput(
        "contexts",
        "iglu:com.snowplowanalytics.snowplow/geolocation_context/jsonschema/1-1-*",
        "*.invalidJsonPath").some)
    val invalidJsonFieldInput = Input(
      1,
      pojo = None,
      json = JsonInput(
        "invalid_field",
        "iglu:com.snowplowanalytics.snowplow/geolocation_context/jsonschema/1-1-*",
        "$.validJsonPath").some)
    val pojoInput = Input(1, pojo = PojoInput("app_id").some, json = None)

    val templateContext = Input.buildPlaceholderMap(
      List(invalidJsonPathInput, pojoInput, invalidJsonFieldInput),
      null,
      derivedContexts = Nil,
      customContexts = List(Json.fromValues(Nil)),
      unstructEvent = None)
    templateContext must beFailing.like {
      case errors => errors.toList must have length 3
    }
  }

  def e5 = {
    val event = new EnrichedEvent
    val pojoInput = Input(1, pojo = PojoInput("app_id").some, json = None)
    val templateContext = pojoInput.getFromEvent(event)
    templateContext must beSuccessful.like {
      case map => map must beEqualTo((1, None))
    }
  }

  def e6 = {
    val event = new EnrichedEvent
    val pojoInput = Input(1, pojo = PojoInput("unknown_property").some, json = None)
    val templateContext = pojoInput.getFromEvent(event)
    templateContext must beFailing
  }

  def e9 = {
    val event = new EnrichedEvent
    val placeholderMap = Input.buildPlaceholderMap(List(), event, Nil, Nil, None)
    placeholderMap must beSuccessful.like {
      case opt =>
        opt must beSome.like {
          case map => map must beEqualTo(IntMap.empty[Input.PlaceholderMap])
        }
    }
  }

  /**
   * This test checks if we have [[StatementPlaceholder]] for all properties
   * in [[EnrichedEvent]]. This test will fail if someone added field with
   * unknown type to [[EnrichedEvent]] at the same time not adding
   * corresponding [[StatementPlaceholder]]
   */
  def e10 =
    eventTypeMap.values.toSet.diff(typeHandlersMap.keySet) must beEmpty

  def e7 = {
    val jsonObject = Input.extractFromJson(json"""{"foo": "bar"} """)
    val jsonNull = Input.extractFromJson(json"null")
    val jsonBool = Input.extractFromJson(json"true")
    val jsonBigInt =
      Input.extractFromJson(parse((java.lang.Long.MAX_VALUE - 1).toString).toOption.get)

    val o = jsonObject must beNone
    val n = jsonNull must beNone
    val b = jsonBool must beSome(Input.BooleanPlaceholder.Value(true))
    val l = jsonBigInt must beSome(Input.LongPlaceholder.Value(java.lang.Long.MAX_VALUE - 1))

    o.and(n).and(b).and(l)
  }

  def e11 = {
    val event = new EnrichedEvent
    event.setApp_id("enrichment-test")
    event.setBr_viewwidth(800)
    event.setGeo_longitude(32.3f)

    val appid = Input(3, Some(PojoInput("app_id")), None).getFromEvent(event) must beSuccessful
      .like {
        case (3, Some(StringPlaceholder.Value("enrichment-test"))) => ok
        case _ => ko
      }
    val viewwidth = Input(1, Some(PojoInput("br_viewwidth")), None)
      .getFromEvent(event) must beSuccessful.like {
      case (1, Some(IntPlaceholder.Value(800))) => ok
      case _ => ko
    }
    val longitude = Input(1, Some(PojoInput("geo_longitude")), None)
      .getFromEvent(event) must beSuccessful.like {
      case (1, Some(FloatPlaceholder.Value(32.3f))) => ok
      case _ => ko
    }

    appid.and(viewwidth).and(longitude)
  }
}
