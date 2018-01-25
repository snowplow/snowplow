/*
 * Copyright (c) 2012-2018 Snowplow Analytics Ltd. All rights reserved.
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
package enrichments
package registry
package apirequest

// json4s
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.{ parseJson, prettyJson }
import org.json4s.jackson.JsonMethods.asJsonNode

// specs2
import org.specs2.Specification
import org.specs2.scalaz.ValidationMatchers
import org.specs2.matcher.Matcher

// Iglu
import com.snowplowanalytics.iglu.client.{ JsonSchemaPair, SchemaKey }

// This project
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
  def createPair(key: SchemaKey, validJson: String): JsonSchemaPair = {
    val hierarchy = parseJson(s"""{"rootId":null,"rootTstamp":null,"refRoot":"events","refTree":["events","${key.name}"],"refParent":"events"}""")
    (key, asJsonNode(("data", parseJson(validJson)) ~ ("hierarchy", hierarchy) ~ ("schema", key.toJValue)))
  }
}

import ApiRequestEnrichmentIntegrationTest._
class ApiRequestEnrichmentIntegrationTest extends Specification with ValidationMatchers { def is =
  skipAllUnless(continuousIntegration) ^
  s2"""
  This is a integration test for the ApiRequestEnrichment
  Basic Case                                      $e1
  POST, Auth, JSON inputs, cache, several outputs $e2
  """

  object IntegrationTests {
    val configuration = parseJson("""
        |{
        |
        |    "vendor": "com.snowplowanalytics.snowplow.enrichments",
        |    "name": "api_request_enrichment_config",
        |    "enabled": true,
        |    "parameters": {
        |      "inputs": [
        |        {
        |          "key": "user",
        |          "pojo": {
        |            "field": "user_id"
        |          }
        |        },
        |        {
        |          "key": "client",
        |          "pojo": {
        |            "field": "app_id"
        |          }
        |        }
        |      ],
        |      "api": {
        |        "http": {
        |          "method": "GET",
        |          "uri": "http://localhost:8000/guest/api/{{client}}/{{user}}?format=json",
        |         "timeout": 5000,
        |          "authentication": {}
        |        }
        |      },
        |      "outputs": [{
        |        "schema": "iglu:com.acme/unauth/jsonschema/1-0-0",
        |        "json": {
        |          "jsonPath": "$"
        |        }
        |      }],
        |      "cache": {
        |        "size": 3000,
        |        "ttl": 60
        |      }
        |    }
        |  }
        |
      """.stripMargin)

    val correctResultContext = parseJson(
      """
        |{
        |  "schema": "iglu:com.acme/unauth/jsonschema/1-0-0",
        |  "data": {"path": "/guest/api/lookup-test/snowplower?format=json", "message": "unauthorized", "method": "GET"}
        |}
      """.stripMargin)


    val configuration2 = parseJson("""
        |{
        |    "vendor": "com.snowplowanalytics.snowplow.enrichments",
        |    "name": "api_request_enrichment_config",
        |    "enabled": true,
        |    "parameters": {
        |      "inputs": [
        |      {
        |        "key": "user",
        |        "pojo": {
        |          "field": "user_id"
        |        }
        |      },
        |      {
        |        "key": "client",
        |        "pojo": {
        |          "field": "app_id"
        |       }
        |      },
        |      {
        |        "key": "jobflow",
        |        "json": {
        |          "field": "unstruct_event",
        |          "jsonPath": "$.jobflow_id",
        |          "schemaCriterion": "iglu:com.snowplowanalytics.monitoring.batch/emr_job_status/jsonschema/*-*-*"
        |        }
        |      },
        |      {
        |       "key": "latitude",
        |       "json": {
        |         "field": "contexts",
        |         "jsonPath": "$.latitude",
        |         "schemaCriterion": "iglu:com.snowplowanalytics.snowplow/geolocation_context/jsonschema/1-1-0"
        |      }
        |      },
        |      {
        |        "key": "datetime",
        |        "json": {
        |          "field": "derived_contexts",
        |          "jsonPath": "$.dt",
        |          "schemaCriterion": "iglu:org.openweathermap/weather/jsonschema/1-*-*"
        |        }
        |      }],
        |      "api": {
        |        "http": {
        |          "method": "POST",
        |          "uri": "http://localhost:8000/api/{{client}}/{{user}}/{{ jobflow }}/{{latitude}}?date={{ datetime }}",
        |         "timeout": 5000,
        |          "authentication": {
        |            "httpBasic": {
        |              "username": "snowplower",
        |              "password": "supersecret"
        |           }
        |         }
        |        }
        |      },
        |      "outputs": [{
        |        "schema": "iglu:com.acme/user/jsonschema/1-0-0",
        |        "json": {
        |          "jsonPath": "$.data.lookupArray[0]"
        |        }
        |      }, {
        |        "schema": "iglu:com.acme/onlypath/jsonschema/1-0-0",
        |        "json": {
        |          "jsonPath": "$.data.lookupArray[1]"
        |        }
        |      }],
        |      "cache": {
        |        "size": 3000,
        |        "ttl": 60
        |      }
        |    }
        |  }
        |
      """.stripMargin)

    // NOTE: akka-http 1.0 was sending "2014-11-10T08:38:30.000Z" as is with ':', this behavior was changed in 2.0

    val correctResultContext2 = parseJson("""
     |{
     |  "schema": "iglu:com.acme/user/jsonschema/1-0-0",
     |  "data": {"path": "/api/lookup+test/snowplower/j-ZKIY4CKQRX72/32.1?date=2014-11-10T08%3A38%3A30.000Z","method": "POST", "auth_header": "snowplower:supersecret", "request": 1}
     |}
    """.stripMargin)

    val correctResultContext3 = parseJson("""
     |{
     |  "schema": "iglu:com.acme/onlypath/jsonschema/1-0-0",
     |  "data": {"path": "/api/lookup+test/snowplower/j-ZKIY4CKQRX72/32.1?date=2014-11-10T08%3A38%3A30.000Z", "request": 1}
     |}
     """.stripMargin)

    // Usual self-describing instance
    val weatherContext = parseJson("""
      |{
      | "schema": "iglu:org.openweathermap/weather/jsonschema/1-0-0",
      | "data": {
      |    "clouds": {
      |        "all": 0
      |    },
      |    "dt": "2014-11-10T08:38:30.000Z",
      |    "main": {
      |        "grnd_level": 1021.91,
      |        "humidity": 90,
      |        "pressure": 1021.91,
      |        "sea_level": 1024.77,
      |        "temp": 301.308,
      |        "temp_max": 301.308,
      |        "temp_min": 301.308
      |    },
      |    "weather": [ { "description": "Sky is Clear", "icon": "01d", "id": 800, "main": "Clear" } ],
      |    "wind": {
      |        "deg": 190.002,
      |        "speed": 4.39
      |    }
      |}
      |}
    """.stripMargin).asInstanceOf[JObject]

    // JsonSchemaPair built by Shredder
    val customContexts = createPair(SchemaKey("com.snowplowanalytics.snowplow", "geolocation_context", "jsonschema", "1-1-0"),
      """
        |{"latitude": 32.1, "longitude": 41.1}
      """.stripMargin)

    // JsonSchemaPair built by Shredder
    val unstructEvent = createPair(SchemaKey("com.snowplowanalytics.monitoring.batch", "emr_job_status", "jsonschema", "1-0-0"),
      """
        |{"name": "Snowplow ETL", "jobflow_id": "j-ZKIY4CKQRX72", "state": "RUNNING", "created_at": "2016-01-21T13:14:10.193+03:00"}
      """.stripMargin)
  }

  val SCHEMA_KEY = SchemaKey("com.snowplowanalytics.snowplow.enrichments", "api_request_enrichment_config", "jsonschema", "1-0-0")

  /**
   * Helper matcher to print JSON
   */
  def beJson(expected: JValue): Matcher[JValue] = { actual: JValue =>
    (actual == expected,
      "actual:\n" + prettyJson(actual) + "\n expected:\n" + prettyJson(expected) + "\n")
  }

  def e1 = {
    val config = ApiRequestEnrichmentConfig.parse(IntegrationTests.configuration, SCHEMA_KEY)
    val event = new EnrichedEvent
    event.setApp_id("lookup-test")
    event.setUser_id("snowplower")
    val context = config.flatMap(_.lookup(event, Nil, Nil, Nil))
    context must beSuccessful.like {
      case context => context must contain(IntegrationTests.correctResultContext) and(context must have size(1))
    }
  }

  def e2 = {
    val config = ApiRequestEnrichmentConfig.parse(IntegrationTests.configuration2, SCHEMA_KEY)
    val event = new EnrichedEvent
    event.setApp_id("lookup test")
    event.setUser_id("snowplower")

    // Fill cache
    config.flatMap(_.lookup(event,
      List(IntegrationTests.weatherContext),
      List(IntegrationTests.customContexts),
      List(IntegrationTests.unstructEvent)))
    config.flatMap(_.lookup(event,
      List(IntegrationTests.weatherContext),
      List(IntegrationTests.customContexts),
      List(IntegrationTests.unstructEvent)))

    val context = config.flatMap(_.lookup(event,
      List(IntegrationTests.weatherContext),
      List(IntegrationTests.customContexts),
      List(IntegrationTests.unstructEvent)))

    context must beSuccessful.like {
      case context => context must contain(beJson(IntegrationTests.correctResultContext2), beJson(IntegrationTests.correctResultContext3)) and(context must have size(2))
    } and { config must beSuccessful.like {
      case c => c.cache.actualLoad must beEqualTo(1)
    }}
  }
}
