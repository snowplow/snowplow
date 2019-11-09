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

import io.circe.literal._
import io.circe.parser._

import com.snowplowanalytics.iglu.core.{SchemaCriterion, SchemaKey, SchemaVer, SelfDescribingData}

import org.specs2.Specification
import org.specs2.matcher.ValidatedMatchers

import outputs.EnrichedEvent

class InputSpec extends Specification with ValidatedMatchers {
  def is = s2"""
  create template context from POJO inputs                 $e1
  create template context from JSON inputs                 $e8
  encountered null in JSON means absent value              $e2
  colliding inputs from JSON and POJO didn't get merged    $e3
  collect failures                                         $e4
  POJO null value return successful None                   $e5
  POJO invalid key return failure                          $e6
  extract correct path-dependent values from JSON          $e7
  create Some empty IntMap for empty list of Inputs        $e9
  check all EnrichedEvent properties can be handled        $e10
  extract correct path-dependent values from EnrichedEvent $e11
  getBySchemaCriterion should return a data payload        $e12
  """

  object ContextCase {
    val ccInput =
      Input.Json(
        1,
        "contexts",
        SchemaCriterion("org.ietf", "http_cookie", "jsonschema", 1),
        "$.value"
      )
    val derInput =
      Input.Json(
        2,
        "derived_contexts",
        SchemaCriterion("org.openweathermap", "weather", "jsonschema", 1, 0),
        "$.main.humidity"
      )
    val unstructInput =
      Input.Json(
        3,
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
    val overrideHumidityInput =
      Input.Json(
        2,
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

    val cookieContextWithoutNull =
      SelfDescribingData(
        SchemaKey("org.ietf", "http_cookie", "jsonschema", SchemaVer.Full(1, 0, 0)),
        json"""{"name": "someCookieAgain", "value": "someValue"}"""
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
    val input1 = Input.Pojo(1, "user_id")
    val input2 = Input.Pojo(2, "true_tstamp")
    val event = new EnrichedEvent
    event.setUser_id("chuwy")
    event.setTrue_tstamp("20")

    val placeholderMap = Input.buildPlaceholderMap(List(input1, input2), event, Nil, Nil, None)
    placeholderMap must beRight(
      Some(
        IntMap(
          1 -> Input.StringPlaceholder.Value("chuwy"),
          2 -> Input.StringPlaceholder.Value("20")
        )
      )
    )
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
    placeholderMap must beRight(None)
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

    placeholderMap must beRight(
      Some(
        IntMap(
          1 -> Input.StringPlaceholder.Value("someValue"),
          2 -> Input.DoublePlaceholder.Value(43.1d),
          3 -> Input.StringPlaceholder.Value("COMPLETED")
        )
      )
    )
  }

  def e3 = {
    import ContextCase._
    val jsonLatitudeInput = Input.Json(
      1,
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
    val pojoLatitudeInput = Input.Pojo(1, "geo_latitude")
    val event = new EnrichedEvent
    event.setGeo_latitude(42.0f)

    // In API Enrichment this colliding wrong
    val templateContext = Input.buildPlaceholderMap(
      List(pojoLatitudeInput, jsonLatitudeInput),
      event,
      derivedContexts = Nil,
      customContexts = List(overriderContext),
      unstructEvent = None
    )
    templateContext must beRight(Some(IntMap(1 -> Input.DoublePlaceholder.Value(43.1))))
  }

  def e4 = {
    val invalidJsonPathInput =
      Input.Json(
        1,
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
    val invalidJsonFieldInput =
      Input.Json(
        1,
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
    val pojoInput = Input.Pojo(1, "app_id")

    val templateContext = Input.buildPlaceholderMap(
      List(invalidJsonPathInput, pojoInput, invalidJsonFieldInput),
      null,
      derivedContexts = Nil,
      customContexts = Nil,
      unstructEvent = None
    )
    templateContext must beLeft.like {
      case errors => errors.toList must have length 3
    }
  }

  def e5 = {
    val event = new EnrichedEvent
    val pojoInput = Input.Pojo(1, "app_id")
    val templateContext = pojoInput.pull(event, Nil, Nil, None)
    templateContext must beValid((1, None))
  }

  def e6 = {
    val event = new EnrichedEvent
    val pojoInput = Input.Pojo(1, "unknown_property")
    val templateContext = pojoInput.pull(event, Nil, Nil, None)
    templateContext must beInvalid
  }

  def e9 = {
    val event = new EnrichedEvent
    val placeholderMap = Input.buildPlaceholderMap(List(), event, Nil, Nil, None)
    placeholderMap must beRight(Some(IntMap.empty[Input.PlaceholderMap]))
  }

  /**
   * This test checks if we have `StatementPlaceholder` for all properties
   * in `EnrichedEvent`. This test will fail if someone added field with
   * unknown type to `EnrichedEvent` at the same time not adding
   * corresponding `StatementPlaceholder`
   */
  def e10 =
    Input.eventTypeMap.values.toSet.diff(Input.typeHandlersMap.keySet) must beEmpty

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

    val appid = Input.Pojo(3, "app_id").pull(event, Nil, Nil, None) must beValid(
      (3, Some(Input.StringPlaceholder.Value("enrichment-test")))
    )
    val viewwidth = Input
      .Pojo(1, "br_viewwidth")
      .pull(event, Nil, Nil, None) must beValid((1, Some(Input.IntPlaceholder.Value(800))))
    val longitude = Input
      .Pojo(1, "geo_longitude")
      .pull(event, Nil, Nil, None) must beValid((1, Some(Input.FloatPlaceholder.Value(32.3f))))

    appid.and(viewwidth).and(longitude)
  }

  def e12 = {
    val result = Input.getBySchemaCriterion(
      List(ContextCase.derivedContext1, ContextCase.overriderContext),
      SchemaCriterion(
        "com.snowplowanalytics.snowplow",
        "geolocation_context",
        "jsonschema",
        1,
        1
      )
    )

    result must beSome(ContextCase.overriderContext.data)
  }
}
