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

import com.snowplowanalytics.iglu.client.{JsonSchemaPair, SchemaKey}
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.parseJson
import org.json4s.jackson.JsonMethods.asJsonNode
import org.specs2.Specification
import org.specs2.scalaz.ValidationMatchers

import outputs.EnrichedEvent

object SqlQueryEnrichmentIntegrationTest {
  def continuousIntegration: Boolean = sys.env.get("CI") match {
    case Some("true") => true
    case _            => false
  }

  /**
   * Helper function creating almost real [[JsonSchemaPair]] for context/unstruct event
   * out of *valid* JSON string and [[SchemaKey]].
   * Useful only if we're passing unstruct event or custom context (but not derived) straight into
   * SqlQueryEnrichment.lookup method
   *
   * WARNING: this is REQUIRED to test custom contexts (not derived!) and unstruct event
   */
  def createPair(key: SchemaKey, validJson: String): JsonSchemaPair = {
    val hierarchy = parseJson(
      s"""{"rootId":null,"rootTstamp":null,"refRoot":"events","refTree":["events","${key.name}"],"refParent":"events"}""")
    (key, asJsonNode(("data", parseJson(validJson)) ~
      (("hierarchy", hierarchy)) ~
      (("schema", key.toJValue))))
  }

  def createDerived(key: SchemaKey, validJson: String): JObject =
    (("schema", key.toSchemaUri)) ~ (("data", parseJson(validJson)))
}

import SqlQueryEnrichmentIntegrationTest._
class SqlQueryEnrichmentIntegrationTest extends Specification with ValidationMatchers {
  def is =
    skipAllUnless(continuousIntegration) ^
      s2"""
  This is an integration test for the SqlQueryEnrichment
  Basic case        $e1
  All-features test $e2
  """

  val SCHEMA_KEY =
    SchemaKey("com.snowplowanalytics.snowplow.enrichments", "sql_query_enrichment_config", "jsonschema", "1-0-0")

  def e1 = {
    val configuration = parseJson("""
        |{
        |  "vendor": "com.snowplowanalytics.snowplow.enrichments",
        |  "name": "sql_query_enrichment_config",
        |  "enabled": true,
        |  "parameters": {
        |    "inputs": [],
        |    "database": {
        |      "postgresql": {
        |        "host": "localhost",
        |        "port": 5432,
        |        "sslMode": false,
        |        "username": "enricher",
        |        "password": "supersecret1",
        |        "database": "sql_enrichment_test"
        |      }
        |    },
        |    "query": {
        |      "sql": "SELECT 42 AS \"singleColumn\""
        |    },
        |    "output": {
        |      "expectedRows": "AT_MOST_ONE",
        |      "json": {
        |        "schema": "iglu:com.acme/singleColumn/jsonschema/1-0-0",
        |        "describes": "ALL_ROWS",
        |        "propertyNames": "AS_IS"
        |      }
        |    },
        |    "cache": {
        |      "size": 3000,
        |      "ttl": 60
        |    }
        |  }
        |}
      """.stripMargin)

    val event = new EnrichedEvent

    val config  = SqlQueryEnrichmentConfig.parse(configuration, SCHEMA_KEY)
    val context = config.flatMap(_.lookup(event, Nil, Nil, Nil))

    val correctContext = parseJson("""
        |{
        |  "schema": "iglu:com.acme/singleColumn/jsonschema/1-0-0",
        |  "data": {
        |    "singleColumn": 42
        |  }
        |}
      """.stripMargin)

    context must beSuccessful.like {
      case List(json) => json must beEqualTo(correctContext)
    }
  }

  /**
   * Most complex test, it tests:
   * + POJO inputs
   * + unstruct event inputs
   * + derived and custom contexts
   * + colliding inputs
   * + cache
   */
  def e2 = {

    val configuration = parseJson(
      """
        |{
        |  "vendor": "com.snowplowanalytics.snowplow.enrichments",
        |  "name": "sql_query_enrichment_config",
        |  "enabled": true,
        |  "parameters": {
        |    "inputs": [
        |      {
        |        "placeholder": 1,
        |        "pojo": {
        |          "field": "geo_city"
        |        }
        |      },
        |
        |      {
        |        "placeholder": 2,
        |        "json": {
        |          "field": "derived_contexts",
        |          "schemaCriterion": "iglu:org.openweathermap/weather/jsonschema/*-*-*",
        |          "jsonPath": "$.dt"
        |        }
        |      },
        |
        |      {
        |        "placeholder": 3,
        |        "pojo": {
        |          "field": "user_id"
        |        }
        |      },
        |
        |      {
        |        "placeholder": 3,
        |        "json": {
        |           "field": "contexts",
        |           "schemaCriterion": "iglu:com.snowplowanalytics.snowplow/client_session/jsonschema/1-*-*",
        |           "jsonPath": "$.userId"
        |         }
        |      },
        |
        |      {
        |        "placeholder": 4,
        |        "json": {
        |           "field": "contexts",
        |           "schemaCriterion": "iglu:com.snowplowanalytics.snowplow/geolocation_context/jsonschema/1-1-*",
        |           "jsonPath": "$.speed"
        |        }
        |      },
        |
        |      {
        |        "placeholder": 5,
        |        "json": {
        |           "field": "unstruct_event",
        |           "schemaCriterion": "iglu:com.snowplowanalytics.monitoring.kinesis/app_initialized/jsonschema/1-0-0",
        |           "jsonPath": "$.applicationName"
        |        }
        |      }
        |    ],
        |
        |    "database": {
        |      "postgresql": {
        |        "host": "localhost",
        |        "port": 5432,
        |        "sslMode": false,
        |        "username": "enricher",
        |        "password": "supersecret1",
        |        "database": "sql_enrichment_test"
        |      }
        |    },
        |    "query": {
        |      "sql": "SELECT city, country, pk FROM enrichment_test WHERE city = ? AND date_time = ? AND name = ? AND speed = ? AND aux = ?;"
        |    },
        |    "output": {
        |      "expectedRows": "AT_MOST_ONE",
        |      "json": {
        |        "schema": "iglu:com.acme/demographic/jsonschema/1-0-0",
        |        "describes": "ALL_ROWS",
        |        "propertyNames": "CAMEL_CASE"
        |      }
        |    },
        |    "cache": {
        |      "size": 3000,
        |      "ttl": 60
        |    }
        |  }
        |}
      """.stripMargin)

    val event1 = new EnrichedEvent
    event1.setGeo_city("Krasnoyarsk")
    val weatherContext1 = createDerived(
      SchemaKey("org.openweathermap", "weather", "jsonschema", "1-0-0"),
      """{"main":{"humidity":78.0,"pressure":1010.0,"temp":260.91,"temp_min":260.15,"temp_max":261.15},"wind":{"speed":2.0,"deg":250.0,"var_end":270,"var_beg":200},"clouds":{"all":75},"weather":[{"main":"Snow","description":"light snow","id":600,"icon":"13d"},{"main":"Mist","description":"mist","id":701,"icon":"50d"}],"dt":"2016-01-07T10:10:34.000Z"}"""
    )
    event1.setUser_id("alice")
    val geoContext1 = createPair(
      SchemaKey("com.snowplowanalytics.snowplow", "geolocation_context", "jsonschema", "1-1-0"),
      """ {"latitude": 12.5, "longitude": 32.1, "speed": 10.0} """)
    val ue1 = createPair(
      SchemaKey("com.snowplowanalytics.monitoring.kinesis", "app_initialized", "jsonschema", "1-0-0"),
      """ {"applicationName": "ue_test_krsk"} """)

    val event2 = new EnrichedEvent
    event2.setGeo_city("London")
    val weatherContext2 = createDerived(
      SchemaKey("org.openweathermap", "weather", "jsonschema", "1-0-0"),
      """{"main":{"humidity":78.0,"pressure":1010.0,"temp":260.91,"temp_min":260.15,"temp_max":261.15},"wind":{"speed":2.0,"deg":250.0,"var_end":270,"var_beg":200},"clouds":{"all":75},"weather":[{"main":"Snow","description":"light snow","id":600,"icon":"13d"},{"main":"Mist","description":"mist","id":701,"icon":"50d"}],"dt":"2016-01-08T10:00:34.000Z"}"""
    )
    event2.setUser_id("bob")
    val geoContext2 = createPair(
      SchemaKey("com.snowplowanalytics.snowplow", "geolocation_context", "jsonschema", "1-1-0"),
      """ {"latitude": 12.5, "longitude": 32.1, "speed": 25.0} """)
    val ue2 = createPair(
      SchemaKey("com.snowplowanalytics.monitoring.kinesis", "app_initialized", "jsonschema", "1-0-0"),
      """ {"applicationName": "ue_test_london"} """)

    val event3 = new EnrichedEvent
    event3.setGeo_city("New York")
    val weatherContext3 = createDerived(
      SchemaKey("org.openweathermap", "weather", "jsonschema", "1-0-0"),
      """{"main":{"humidity":78.0,"pressure":1010.0,"temp":260.91,"temp_min":260.15,"temp_max":261.15},"wind":{"speed":2.0,"deg":250.0,"var_end":270,"var_beg":200},"clouds":{"all":75},"weather":[{"main":"Snow","description":"light snow","id":600,"icon":"13d"},{"main":"Mist","description":"mist","id":701,"icon":"50d"}],"dt":"2016-02-07T10:10:00.000Z"}"""
    )
    event3.setUser_id("eve")
    val geoContext3 = createPair(
      SchemaKey("com.snowplowanalytics.snowplow", "geolocation_context", "jsonschema", "1-1-0"),
      """ {"latitude": 12.5, "longitude": 32.1, "speed": 2.5} """)
    val ue3 = createPair(
      SchemaKey("com.snowplowanalytics.monitoring.kinesis", "app_initialized", "jsonschema", "1-0-0"),
      """ {"applicationName": "ue_test_ny"} """)

    val event4 = new EnrichedEvent
    event4.setGeo_city("London")
    val weatherContext4 = createDerived(
      SchemaKey("org.openweathermap", "weather", "jsonschema", "1-0-0"),
      """{"main":{"humidity":78.0,"pressure":1010.0,"temp":260.91,"temp_min":260.15,"temp_max":261.15},"wind":{"speed":2.0,"deg":250.0,"var_end":270,"var_beg":200},"clouds":{"all":75},"weather":[{"main":"Snow","description":"light snow","id":600,"icon":"13d"},{"main":"Mist","description":"mist","id":701,"icon":"50d"}],"dt":"2016-01-08T10:00:34.000Z"}"""
    )
    event4.setUser_id("eve") // This should be ignored because of clientSession4
    val clientSession4 = createPair(
      SchemaKey("com.snowplowanalytics.snowplow", "client_session", "jsonschema", "1-0-1"),
      """ { "userId": "bob", "sessionId": "123e4567-e89b-12d3-a456-426655440000", "sessionIndex": 1, "previousSessionId": null, "storageMechanism": "SQLITE" } """
    )
    val geoContext4 = createPair(
      SchemaKey("com.snowplowanalytics.snowplow", "geolocation_context", "jsonschema", "1-1-0"),
      """ {"latitude": 12.5, "longitude": 32.1, "speed": 25.0} """)
    val ue4 = createPair(
      SchemaKey("com.snowplowanalytics.monitoring.kinesis", "app_initialized", "jsonschema", "1-0-0"),
      """ {"applicationName": "ue_test_london"} """)

    val config = SqlQueryEnrichmentConfig.parse(configuration, SCHEMA_KEY)

    val context1        = config.flatMap(_.lookup(event1, List(weatherContext1), List(geoContext1), List(ue1)))
    val result_context1 = parseJson("""|{"schema":"iglu:com.acme/demographic/jsonschema/1-0-0",
        |  "data": {
        |    "city": "Krasnoyarsk",
        |    "country": "Russia",
        |    "pk": 1}}""".stripMargin)

    val context2        = config.flatMap(_.lookup(event2, List(weatherContext2), List(geoContext2), List(ue2)))
    val result_context2 = parseJson("""|{"schema":"iglu:com.acme/demographic/jsonschema/1-0-0",
        |  "data": {
        |    "city": "London",
        |    "country": "England",
        |    "pk": 2}}""".stripMargin)

    val context3        = config.flatMap(_.lookup(event3, List(weatherContext3), List(geoContext3), List(ue3)))
    val result_context3 = parseJson("""|{"schema":"iglu:com.acme/demographic/jsonschema/1-0-0",
        |  "data": {
        |    "city": "New York",
        |    "country": "USA",
        |    "pk": 3}}
      """.stripMargin)

    val context4        = config.flatMap(_.lookup(event4, List(weatherContext4), List(geoContext4, clientSession4), List(ue4)))
    val result_context4 = parseJson("""|{"schema":"iglu:com.acme/demographic/jsonschema/1-0-0",
        |  "data": {
        |    "city": "London",
        |    "country": "England",
        |    "pk": 2}}""".stripMargin)

    val res1  = context1 must beSuccessful.like { case List(ctx)                  => ctx must beEqualTo(result_context1) }
    val res2  = context2 must beSuccessful.like { case List(ctx)                  => ctx must beEqualTo(result_context2) }
    val res3  = context3 must beSuccessful.like { case List(ctx)                  => ctx must beEqualTo(result_context3) }
    val res4  = context4 must beSuccessful.like { case List(ctx)                  => ctx must beEqualTo(result_context4) }
    val cache = config.map(_.cache.actualLoad) must beSuccessful.like { case size => size must beEqualTo(3) }

    res1.and(res2).and(res3).and(res4).and(cache)
  }
}
