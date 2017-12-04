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
package com.snowplowanalytics.snowplow.enrich.common.enrichments.registry.apirequest

// json4s
import org.json4s._
import org.json4s.jackson.parseJson

// specs2
import org.specs2.Specification
import org.specs2.scalaz.ValidationMatchers

// Iglu
import com.snowplowanalytics.iglu.client.SchemaKey

class ApiRequestEnrichmentSpec extends Specification with ValidationMatchers {
  def is = s2"""
  This is a specification to test the ApiRequestEnrichment configuration
  Extract correct configuration                                $e1
  Skip incorrect input (none of json or pojo) in configuration $e2
  Skip incorrect input (both json and pojo) in configuration   $e3
  """"

  val SCHEMA_KEY =
    SchemaKey("com.snowplowanalytics.snowplow.enrichments", "api_request_enrichment_config", "jsonschema", "1-0-0")

  def e1 = {
    val inputs = List(
      Input("user", pojo = Some(PojoInput("user_id")), json = None),
      Input(
        "user",
        pojo = None,
        json = Some(
          JsonInput("contexts", "iglu:com.snowplowanalytics.snowplow/client_session/jsonschema/1-*-*", "$.userId"))),
      Input("client", pojo = Some(PojoInput("app_id")), json = None)
    )
    val api =
      HttpApi("GET",
              "http://api.acme.com/users/{{client}}/{{user}}?format=json",
              1000,
              Authentication(Some(HttpBasic(Some("xxx"), None))))
    val output = Output("iglu:com.acme/user/jsonschema/1-0-0", Some(JsonOutput("$.record")))
    val cache  = Cache(3000, 60)
    val config = ApiRequestEnrichment(inputs, api, List(output), cache)

    val configuration = parseJson(
      """|{
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
      |          "key": "user",
      |          "json": {
      |            "field": "contexts",
      |            "schemaCriterion": "iglu:com.snowplowanalytics.snowplow/client_session/jsonschema/1-*-*",
      |            "jsonPath": "$.userId"
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
      |          "uri": "http://api.acme.com/users/{{client}}/{{user}}?format=json",
      |          "timeout": 1000,
      |          "authentication": {
      |            "httpBasic": {
      |              "username": "xxx",
      |              "password": null
      |            }
      |          }
      |        }
      |      },
      |      "outputs": [{
      |        "schema": "iglu:com.acme/user/jsonschema/1-0-0",
      |        "json": {
      |          "jsonPath": "$.record"
      |        }
      |      }],
      |      "cache": {
      |        "size": 3000,
      |        "ttl": 60
      |      }
      |    }
      |  }""".stripMargin)

    ApiRequestEnrichmentConfig.parse(configuration, SCHEMA_KEY) must beSuccessful(config)
  }

  def e2 = {
    val configuration = parseJson("""|{
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
      |          "key": "user"
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
      |          "uri": "http://api.acme.com/users/{{client}}/{{user}}?format=json",
      |          "timeout": 1000,
      |          "authentication": {
      |            "httpBasic": {
      |              "username": "xxx",
      |              "password": "yyy"
      |            }
      |          }
      |        }
      |      },
      |      "outputs": [{
      |        "schema": "iglu:com.acme/user/jsonschema/1-0-0",
      |        "json": {
      |          "jsonPath": "$.record"
      |        }
      |      }],
      |      "cache": {
      |        "size": 3000,
      |        "ttl": 60
      |      }
      |    }
      |  }""".stripMargin)
    ApiRequestEnrichmentConfig.parse(configuration, SCHEMA_KEY) must beFailing
  }

  def e3 = {
    val configuration = parseJson(
      """|{
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
      |          },
      |         "json": {
      |            "field": "contexts",
      |            "schemaCriterion": "iglu:com.snowplowanalytics.snowplow/client_session/jsonschema/1-*-*",
      |            "jsonPath": "$.userId"
      |         }
      |        }
      |      ],
      |      "api": {
      |        "http": {
      |          "method": "GET",
      |          "uri": "http://api.acme.com/users/{{client}}/{{user}}?format=json",
      |          "timeout": 1000,
      |          "authentication": {
      |            "httpBasic": {
      |              "username": "xxx",
      |              "password": "yyy"
      |            }
      |          }
      |        }
      |      },
      |      "outputs": [{
      |        "schema": "iglu:com.acme/user/jsonschema/1-0-0",
      |        "json": {
      |          "jsonPath": "$.record"
      |        }
      |      }],
      |      "cache": {
      |        "size": 3000,
      |        "ttl": 60
      |      }
      |    }
      |  }""".stripMargin)
    ApiRequestEnrichmentConfig.parse(configuration, SCHEMA_KEY) must beFailing
  }
}
