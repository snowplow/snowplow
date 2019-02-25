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
package com.snowplowanalytics.snowplow.enrich.common.enrichments.registry.apirequest

// Scalaz
import org.json4s.jackson.JsonMethods
import scalaz.Scalaz._

// json4s
import org.json4s._
import org.json4s.jackson.parseJson

// specs2
import org.specs2.Specification
import org.specs2.mock.Mockito
import org.specs2.scalaz.ValidationMatchers

// Iglu
import com.snowplowanalytics.iglu.client.JsonSchemaPair
import com.snowplowanalytics.iglu.client.SchemaKey

// Snowplow
import com.snowplowanalytics.snowplow.enrich.common.outputs.EnrichedEvent

class ApiRequestEnrichmentSpec extends Specification with ValidationMatchers with Mockito {
  def is = s2"""
  This is a specification to test the ApiRequestEnrichment configuration
  Extract correct configuration for GET request and perform the request  $e1
  Skip incorrect input (none of json or pojo) in configuration           $e2
  Skip incorrect input (both json and pojo) in configuration             $e3
  Extract correct configuration for POST request and perform the request $e4
  """"

  val SCHEMA_KEY =
    SchemaKey("com.snowplowanalytics.snowplow.enrichments", "api_request_enrichment_config", "jsonschema", "1-0-0")

  def e1 = {
    val inputs = List(
      Input("user", pojo = Some(PojoInput("user_id")), json = None),
      Input(
        "userSession",
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
    val apiSpy = spy(api)
    val output = Output("iglu:com.acme/user/jsonschema/1-0-0", Some(JsonOutput("$.record")))
    val cache  = Cache(3000, 60)
    val config = ApiRequestEnrichment(inputs, apiSpy, List(output), cache)

    val fakeEnrichedEvent = new EnrichedEvent {
      app_id  = "some-fancy-app-id"
      user_id = "some-fancy-user-id"
    }

    val clientSession: JsonSchemaPair = (
      SchemaKey(vendor  = "com.snowplowanalytics.snowplow",
                name    = "client_session",
                format  = "jsonschema",
                version = "1-0-1"),
      JsonMethods.asJsonNode(parseJson("""|{
           |    "data": {
           |        "userId": "some-fancy-user-session-id",
           |        "sessionId": "42c8a55b-c0c2-4749-b9ac-09bb0d17d000",
           |        "sessionIndex": 1,
           |        "previousSessionId": null,
           |        "storageMechanism": "COOKIE_1"
           |    }
           |}""".stripMargin))
    )

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
      |          "key": "userSession",
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

    val user = parseJson("""|{
         |    "schema": "iglu:com.acme/user/jsonschema/1-0-0",
         |    "data": {
         |      "name": "Fancy User",
         |      "company": "Acme"
         |    }
         |}
      """.stripMargin)

    apiSpy.perform(
      url  = "http://api.acme.com/users/some-fancy-app-id/some-fancy-user-id?format=json",
      body = None
    ) returns """{"record": {"name": "Fancy User", "company": "Acme"}}""".success

    val enrichedContextResult = config.lookup(
      event           = fakeEnrichedEvent,
      derivedContexts = List.empty,
      customContexts  = List(clientSession),
      unstructEvent   = List.empty
    )

    enrichedContextResult must beSuccessful(List(user))
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

  def e4 = {
    val inputs = List(
      Input(key = "user", pojo = Some(PojoInput("user_id")), json = None),
      Input(
        key  = "userSession",
        pojo = None,
        json = Some(
          JsonInput("contexts", "iglu:com.snowplowanalytics.snowplow/client_session/jsonschema/1-*-*", "$.userId"))),
      Input(key = "client", pojo = Some(PojoInput("app_id")), json = None)
    )

    val api = HttpApi(method = "POST",
                      uri            = "http://api.acme.com/users?format=json",
                      timeout        = 1000,
                      authentication = Authentication(Some(HttpBasic(Some("xxx"), None))))
    val apiSpy = spy(api)
    val output = Output(schema = "iglu:com.acme/user/jsonschema/1-0-0", json = Some(JsonOutput("$.record")))
    val cache  = Cache(size = 3000, ttl = 60)
    val config = ApiRequestEnrichment(inputs, apiSpy, List(output), cache)

    val fakeEnrichedEvent = new EnrichedEvent {
      app_id  = "some-fancy-app-id"
      user_id = "some-fancy-user-id"
    }

    val clientSession: JsonSchemaPair = (
      SchemaKey(vendor  = "com.snowplowanalytics.snowplow",
                name    = "client_session",
                format  = "jsonschema",
                version = "1-0-1"),
      JsonMethods.asJsonNode(parseJson("""|{
           |    "data": {
           |        "userId": "some-fancy-user-session-id",
           |        "sessionId": "42c8a55b-c0c2-4749-b9ac-09bb0d17d000",
           |        "sessionIndex": 1,
           |        "previousSessionId": null,
           |        "storageMechanism": "COOKIE_1"
           |    }
           |}""".stripMargin))
    )

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
         |          "key": "userSession",
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
         |          "method": "POST",
         |          "uri": "http://api.acme.com/users?format=json",
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

    val user = parseJson("""|{
         |    "schema": "iglu:com.acme/user/jsonschema/1-0-0",
         |    "data": {
         |      "name": "Fancy User",
         |      "company": "Acme"
         |    }
         |}
      """.stripMargin)

    apiSpy.perform(
      url = "http://api.acme.com/users?format=json",
      body = Some(
        """{"client":"some-fancy-app-id","user":"some-fancy-user-id","userSession":"some-fancy-user-session-id"}""")
    ) returns """{"record": {"name": "Fancy User", "company": "Acme"}}""".success

    val enrichedContextResult = config.lookup(
      event           = fakeEnrichedEvent,
      derivedContexts = List.empty,
      customContexts  = List(clientSession),
      unstructEvent   = List.empty
    )

    enrichedContextResult must beSuccessful(List(user))
  }
}
