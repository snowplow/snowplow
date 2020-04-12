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
package enrichments.registry.apirequest

import cats.Eval

import io.circe._
import io.circe.literal._

import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer, SelfDescribingData}

import org.specs2.Specification
import org.specs2.matcher.Matcher

import outputs.EnrichedEvent

object ApiRequestEnrichmentIntegrationTest {
  def continuousIntegration: Boolean = sys.env.get("CI") match {
    case Some("true") => true
    case _ => false
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
    val configuration = json"""{
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
            "jsonPath": "$$"
          }
        }],
        "cache": {
          "size": 3000,
          "ttl": 60
        }
      }
    }"""

    val correctResultContext =
      SelfDescribingData(
        SchemaKey("com.acme", "unauth", "jsonschema", SchemaVer.Full(1, 0, 0)),
        json"""{"path": "/guest/api/lookup-test/snowplower?format=json", "message": "unauthorized", "method": "GET"}"""
      )

    val configuration2 = json"""{
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
            "jsonPath": "$$.jobflow_id",
            "schemaCriterion": "iglu:com.snowplowanalytics.monitoring.batch/emr_job_status/jsonschema/*-*-*"
          }
        },
        {
         "key": "latitude",
         "json": {
           "field": "contexts",
           "jsonPath": "$$.latitude",
           "schemaCriterion": "iglu:com.snowplowanalytics.snowplow/geolocation_context/jsonschema/1-1-0"
        }
        },
        {
          "key": "datetime",
          "json": {
            "field": "derived_contexts",
            "jsonPath": "$$.dt",
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
            "jsonPath": "$$.data.lookupArray[0]"
          }
        }, {
          "schema": "iglu:com.acme/onlypath/jsonschema/1-0-0",
          "json": {
            "jsonPath": "$$.data.lookupArray[1]"
          }
        }],
        "cache": {
          "size": 3000,
          "ttl": 60
        }
      }
    }"""

    // NOTE: akka-http 1.0 was sending "2014-11-10T08:38:30.000Z" as is with ':', this behavior was changed in 2.0

    val correctResultContext2 =
      SelfDescribingData(
        SchemaKey("com.acme", "user", "jsonschema", SchemaVer.Full(1, 0, 0)),
        json"""{
          "path": "/api/lookup+test/snowplower/j-ZKIY4CKQRX72/32.1?date=2014-11-10T08%3A38%3A30.000Z",
          "method": "POST",
          "auth_header": "snowplower:supersecret",
          "request": 1
        }"""
      )

    val correctResultContext3 =
      SelfDescribingData(
        SchemaKey("com.acme", "onlypath", "jsonschema", SchemaVer.Full(1, 0, 0)),
        json"""{"path": "/api/lookup+test/snowplower/j-ZKIY4CKQRX72/32.1?date=2014-11-10T08%3A38%3A30.000Z", "request": 1}"""
      )

    // Usual self-describing instance
    val weatherContext =
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

    // JsonSchemaPair built by Shredder
    val customContexts = SelfDescribingData(
      SchemaKey(
        "com.snowplowanalytics.snowplow",
        "geolocation_context",
        "jsonschema",
        SchemaVer.Full(1, 1, 0)
      ),
      json"""{"latitude": 32.1, "longitude": 41.1}"""
    )

    // JsonSchemaPair built by Shredder
    val unstructEvent = SelfDescribingData(
      SchemaKey(
        "com.snowplowanalytics.monitoring.batch",
        "emr_job_status",
        "jsonschema",
        SchemaVer.Full(1, 0, 0)
      ),
      json"""{"name": "Snowplow ETL", "jobflow_id": "j-ZKIY4CKQRX72", "state": "RUNNING", "created_at": "2016-01-21T13:14:10.193+03:00"}"""
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
  def beJson(expected: SelfDescribingData[Json]): Matcher[SelfDescribingData[Json]] = { actual: SelfDescribingData[Json] =>
    {
      val result = actual == expected
      val schemaMatch = actual.schema == expected.schema
      val dataMatch = actual.data == expected.data
      val message =
        if (schemaMatch)
          s"Schema ${actual.schema} matches, data doesn't.\nActual data: ${actual.data.spaces2}\nExpected data: ${expected.data.spaces2}"
        else if (dataMatch)
          s"Data payloads match, schemas don't.\nActual schema: ${actual.schema.toSchemaUri}\nExpected schema: ${expected.schema.toSchemaUri}"
        else "actual:\n" + actual + "\n expected:\n" + expected + "\n"
      (result, message)
    }
  }

  def e1 = {
    val enrichment = ApiRequestEnrichment
      .parse(IntegrationTests.configuration, SCHEMA_KEY)
      .map(_.enrichment[Eval].value)
      .toEither
    val event = new EnrichedEvent
    event.setApp_id("lookup-test")
    event.setUser_id("snowplower")
    val context = enrichment.flatMap(_.lookup(event, Nil, Nil, None).value.toEither)
    context must beRight.like {
      case context =>
        context must contain(IntegrationTests.correctResultContext) and (context must have size 1)
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
        Some(IntegrationTests.unstructEvent)
      ).value.toEither
    )
    enrichment.flatMap(
      _.lookup(
        event,
        List(IntegrationTests.weatherContext),
        List(IntegrationTests.customContexts),
        Some(IntegrationTests.unstructEvent)
      ).value.toEither
    )

    val context = enrichment.flatMap(
      _.lookup(
        event,
        List(IntegrationTests.weatherContext),
        List(IntegrationTests.customContexts),
        Some(IntegrationTests.unstructEvent)
      ).value.toEither
    )

    context must beRight.like {
      case contexts =>
        contexts must contain(
          beJson(IntegrationTests.correctResultContext3),
          beJson(IntegrationTests.correctResultContext2)
        ) and (contexts must have size 2)
    }
  }
}
