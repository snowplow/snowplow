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
package enrichments
package registry
package apirequest

// json4s
import org.json4s.JsonAST._
import org.json4s.jackson.parseJson

// specs2
import org.specs2.Specification
import org.specs2.scalaz.ValidationMatchers

// Iglu
import com.snowplowanalytics.iglu.client.SchemaKey

// This project
import outputs.EnrichedEvent
import utils.JsonUtils

object ApiRequestEnrichmentIntegrationTest {
  def continuousIntegration: Boolean = sys.env.get("CI") match {
    case Some("true") => true
    case _ => false
  }
}

import ApiRequestEnrichmentIntegrationTest._
class ApiRequestEnrichmentIntegrationTest extends Specification with ValidationMatchers { def is =
  "This is a integration test for the ApiRequestEnrichment" ^
    skipAllUnless(continuousIntegration) ^
    "Basic Case"                                       ! e1 ^
    "POST, Auth, JSON inputs, cache, several outputs"  ! e2 ^
                                                       end

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

    val correctContext = parseJson(
      """
        |{
        |  "schema": "iglu:com.acme/unauth/jsonschema/1-0-0",
        |  "data": {"path": "/guest/api/lookup-test/snowplower?format=json", "message": "unatuhorized", "method": "GET"}
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
        |          "field": "unstruct",
        |          "jsonPath": "$.jobflow_id",
        |          "schemaCriterion": "iglu:com.snowplowanalytics.monitoring.batch/emr_job_status/jsonschema/1-0-*"
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

    val correctContext2 = parseJson("""
     |{
     |  "schema": "iglu:com.acme/user/jsonschema/1-0-0",
     |  "data": {"path": "/api/lookup-test/snowplower/j-ZKIY4CKQRX72/32.1?date=2014-11-10T08:38:30.000Z", "auth_header": "snowplower:supersecret", "method": "POST", "request": 1}
     |}
    """.stripMargin)

    val correctContext3 = parseJson("""
     |{
     |  "schema": "iglu:com.acme/onlypath/jsonschema/1-0-0",
     |  "data": {"path": "/api/lookup-test/snowplower/j-ZKIY4CKQRX72/32.1?date=2014-11-10T08:38:30.000Z", "request": 1}
     |}
     """.stripMargin)



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

    val customContexts =
      SchemaKey("com.snowplowanalytics.snowplow", "geolocation_context", "jsonschema", "1-1-0") ->
        JsonUtils.unsafeExtractJson("""{"latitude": 32.1, "longitude": 41.1}"""")

    val unstructEvent =
      SchemaKey("com.snowplowanalytics.monitoring.batch", "emr_job_status", "jsonschema", "1-0-0") ->
        JsonUtils.unsafeExtractJson("""{"name": "Snowplow ETL", "jobflow_id": "j-ZKIY4CKQRX72", "state": "RUNNING", "created_at": "2016-01-21T13:14:10.193+03:00"} """)
  }

  val SCHEMA_KEY = SchemaKey("com.snowplowanalytics.snowplow.enrichments", "api_request_enrichment_config", "jsonschema", "1-0-0")

  def e1 = {
    val config = ApiRequestEnrichmentConfig.parse(IntegrationTests.configuration, SCHEMA_KEY)
    val event = new EnrichedEvent
    event.setApp_id("lookup-test")
    event.setUser_id("snowplower")
    val context = config.flatMap(_.lookup(event, Nil, Nil, Nil))
    context must beSuccessful.like {
      case context => context must contain(IntegrationTests.correctContext) and(context must have size(1))
    }
  }

  def e2 = {
    val config = ApiRequestEnrichmentConfig.parse(IntegrationTests.configuration2, SCHEMA_KEY)
    val event = new EnrichedEvent
    event.setApp_id("lookup-test")
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
      case context => context must contain(IntegrationTests.correctContext2, IntegrationTests.correctContext3) and(context must have size(2))
    } and { config must beSuccessful.like {
      case c => c.cache.actualLoad must beEqualTo(1)
    }}
  }
}
