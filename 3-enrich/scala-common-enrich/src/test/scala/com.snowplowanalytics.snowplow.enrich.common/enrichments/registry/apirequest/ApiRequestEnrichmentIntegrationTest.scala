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

import cats.Eval
import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer, SelfDescribingData}
import io.circe._
import io.circe.generic.auto._
import io.circe.literal._
import io.circe.parser._
import io.circe.syntax._
import org.specs2.Specification
import org.specs2.matcher.Matcher

import outputs.EnrichedEvent

object ApiRequestEnrichmentIntegrationTest {
  def continuousIntegration: Boolean = sys.env.get("CI") match {
    case Some("true") => true
    case _ => false
  }

  /**
   * Helper function creating almost real [[JsonSchemaPair]] for context/unstruct event
   * out of *valid* JSON string and [[SchemaKey]].
   * Useful only if we're passing unstruct event or custom context (but not derived) straight into
   * ApiRequestEnrichment.lookup method
   */
  def createPair(key: SchemaKey, validJson: String): SelfDescribingData[Json] = {
    val hierarchy = parse(
      s"""{"rootId":null,"rootTstamp":null,"refRoot":"events","refTree":["events","${key.name}"],"refParent":"events"}"""
    ).toOption.get
    SelfDescribingData(
      key,
      Json.obj(
        "data" := parse(validJson).toOption.get,
        "hierarchy" := hierarchy,
        "schema" := key.asJson
      )
    )
  }
}

import ApiRequestEnrichmentIntegrationTest._
class ApiRequestEnrichmentIntegrationTest extends Specification {
  def is =
    skipAllUnless(continuousIntegration) ^
      s2"""
  This is a integration test for the ApiRequestEnrichment
  Basic Case                                      $e1
  POST, Auth, JSON inputs, cache, several outputs $e2
  """

  object IntegrationTests {
    val configuration = parse(
      """{
      "vendor": "com.snowplowanalytics.snowplow.enrichments",
      "name": "api_request_enrichment_config",
      "enabled": true,
      "parameters": {
        "inputs": [
          {
            "key": "user",
            "pojo": {
              "field": "user_id"
            }
          },
          {
            "key": "client",
            "pojo": {
              "field": "app_id"
            }
          }
        ],
        "api": {
          "http": {
            "method": "GET",
            "uri": "http://localhost:8000/guest/api/{{client}}/{{user}}?format=json",
           "timeout": 5000,
            "authentication": {}
          }
        },
        "outputs": [{
          "schema": "iglu:com.acme/unauth/jsonschema/1-0-0",
          "json": {
            "jsonPath": "$"
          }
        }],
        "cache": {
          "size": 3000,
          "ttl": 60
        }
      }
    }"""
    ).toOption.get

    val correctResultContext = json"""{
      "schema": "iglu:com.acme/unauth/jsonschema/1-0-0",
      "data": {"path": "/guest/api/lookup-test/snowplower?format=json", "message": "unauthorized", "method": "GET"}
    }"""

    val configuration2 = parse(
      """{
      "vendor": "com.snowplowanalytics.snowplow.enrichments",
      "name": "api_request_enrichment_config",
      "enabled": true,
      "parameters": {
        "inputs": [
        {
          "key": "user",
          "pojo": {
            "field": "user_id"
          }
        },
        {
          "key": "client",
          "pojo": {
            "field": "app_id"
         }
        },
        {
          "key": "jobflow",
          "json": {
            "field": "unstruct_event",
            "jsonPath": "$.jobflow_id",
            "schemaCriterion": "iglu:com.snowplowanalytics.monitoring.batch/emr_job_status/jsonschema/*-*-*"
          }
        },
        {
         "key": "latitude",
         "json": {
           "field": "contexts",
           "jsonPath": "$.latitude",
           "schemaCriterion": "iglu:com.snowplowanalytics.snowplow/geolocation_context/jsonschema/1-1-0"
        }
        },
        {
          "key": "datetime",
          "json": {
            "field": "derived_contexts",
            "jsonPath": "$.dt",
            "schemaCriterion": "iglu:org.openweathermap/weather/jsonschema/1-*-*"
          }
        }],
        "api": {
          "http": {
            "method": "POST",
            "uri": "http://localhost:8000/api/{{client}}/{{user}}/{{ jobflow }}/{{latitude}}?date={{ datetime }}",
           "timeout": 5000,
            "authentication": {
              "httpBasic": {
                "username": "snowplower",
                "password": "supersecret"
             }
           }
          }
        },
        "outputs": [{
          "schema": "iglu:com.acme/user/jsonschema/1-0-0",
          "json": {
            "jsonPath": "$.data.lookupArray[0]"
          }
        }, {
          "schema": "iglu:com.acme/onlypath/jsonschema/1-0-0",
          "json": {
            "jsonPath": "$.data.lookupArray[1]"
          }
        }],
        "cache": {
          "size": 3000,
          "ttl": 60
        }
      }
    }"""
    ).toOption.get

    // NOTE: akka-http 1.0 was sending "2014-11-10T08:38:30.000Z" as is with ':', this behavior was changed in 2.0

    val correctResultContext2 = json"""{
      "schema": "iglu:com.acme/user/jsonschema/1-0-0",
      "data": {"path": "/api/lookup+test/snowplower/j-ZKIY4CKQRX72/32.1?date=2014-11-10T08%3A38%3A30.000Z","method": "POST", "auth_header": "snowplower:supersecret", "request": 1}
    }"""

    val correctResultContext3 = json"""{
      "schema": "iglu:com.acme/onlypath/jsonschema/1-0-0",
      "data": {"path": "/api/lookup+test/snowplower/j-ZKIY4CKQRX72/32.1?date=2014-11-10T08%3A38%3A30.000Z", "request": 1}
    }"""

    // Usual self-describing instance
    val weatherContext = json"""{
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

    // JsonSchemaPair built by Shredder
    val customContexts = createPair(
      SchemaKey(
        "com.snowplowanalytics.snowplow",
        "geolocation_context",
        "jsonschema",
        SchemaVer.Full(1, 1, 0)
      ),
      """
        |{"latitude": 32.1, "longitude": 41.1}
      """.stripMargin
    )

    // JsonSchemaPair built by Shredder
    val unstructEvent = createPair(
      SchemaKey(
        "com.snowplowanalytics.monitoring.batch",
        "emr_job_status",
        "jsonschema",
        SchemaVer.Full(1, 0, 0)
      ),
      """
        |{"name": "Snowplow ETL", "jobflow_id": "j-ZKIY4CKQRX72", "state": "RUNNING", "created_at": "2016-01-21T13:14:10.193+03:00"}
      """.stripMargin
    )
  }

  val SCHEMA_KEY =
    SchemaKey(
      "com.snowplowanalytics.snowplow.enrichments",
      "api_request_enrichment_config",
      "jsonschema",
      SchemaVer.Full(1, 0, 0)
    )

  /**
   * Helper matcher to print JSON
   */
  def beJson(expected: Json): Matcher[Json] = { actual: Json =>
    (actual == expected, "actual:\n" + actual.spaces2 + "\n expected:\n" + expected.spaces2 + "\n")
  }

  def e1 = {
    val enrichment = ApiRequestEnrichment
      .parse(IntegrationTests.configuration, SCHEMA_KEY)
      .map(_.enrichment[Eval].value)
      .toEither
    val event = new EnrichedEvent
    event.setApp_id("lookup-test")
    event.setUser_id("snowplower")
    val context = enrichment.flatMap(_.lookup(event, Nil, Nil, Nil).value.toEither)
    context must beRight.like {
      case context =>
        context must contain(IntegrationTests.correctResultContext) and (context must have size (1))
    }
  }

  def e2 = {
    val enrichment = ApiRequestEnrichment
      .parse(IntegrationTests.configuration2, SCHEMA_KEY)
      .map(_.enrichment[Eval].value)
      .toEither
    val event = new EnrichedEvent
    event.setApp_id("lookup test")
    event.setUser_id("snowplower")

    // Fill cache
    enrichment.flatMap(
      _.lookup(
        event,
        List(IntegrationTests.weatherContext),
        List(IntegrationTests.customContexts),
        List(IntegrationTests.unstructEvent)
      ).value.toEither
    )
    enrichment.flatMap(
      _.lookup(
        event,
        List(IntegrationTests.weatherContext),
        List(IntegrationTests.customContexts),
        List(IntegrationTests.unstructEvent)
      ).value.toEither
    )

    val context = enrichment.flatMap(
      _.lookup(
        event,
        List(IntegrationTests.weatherContext),
        List(IntegrationTests.customContexts),
        List(IntegrationTests.unstructEvent)
      ).value.toEither
    )

    context must beRight.like {
      case context =>
        context must contain(
          beJson(IntegrationTests.correctResultContext2),
          beJson(IntegrationTests.correctResultContext3)
        ) and (context must have size (2))
    }
  }
}
