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
package com.snowplowanalytics.snowplow.enrich.common.enrichments.registry.sqlquery

// json4s
import org.json4s.jackson.parseJson

// specs2
import org.specs2.Specification
import org.specs2.scalaz.ValidationMatchers

// Iglu
import com.snowplowanalytics.iglu.client.SchemaKey

class SqlQueryEnrichmentSpec extends Specification with ValidationMatchers {
  def is = s2"""
  This is a specification to test the SqlQueryEnrichment configuration
  Extract correct configuration       $e1
  Fail to parse invalid configuration $e2
  Extract correct MySQL configuration $e3
  """

  val SCHEMA_KEY =
    SchemaKey("com.snowplowanalytics.snowplow.enrichments", "sql_query_enrichment_config", "jsonschema", "1-0-0")

  def e1 = {
    val inputs = List(
      Input(1, pojo = Some(PojoInput("user_id")), json = None),
      Input(
        1,
        pojo = None,
        json = Some(
          JsonInput("contexts", "iglu:com.snowplowanalytics.snowplow/client_session/jsonschema/1-*-*", "$.userId"))),
      Input(2, pojo = Some(PojoInput("app_id")), json = None)
    )
    val db = Db(
      postgresql = Some(
        PostgresqlDb("cluster01.redshift.acme.com", 5439, sslMode = true, "snowplow_enrich_ro", "1asIkJed", "crm")),
      mysql = None)
    val output = JsonOutput("iglu:com.acme/user/jsonschema/1-0-0", "ALL_ROWS", "CAMEL_CASE")
    val cache  = Cache(3000, 60)
    val query = Query(
      "SELECT username, email_address, date_of_birth FROM tbl_users WHERE user = ? AND client = ? LIMIT 1")
    val config = SqlQueryEnrichment(inputs, db, query, Output(output, "AT_MOST_ONE"), cache)

    val configuration = parseJson(
      """|{
      |    "vendor": "com.snowplowanalytics.snowplow.enrichments",
      |    "name": "sql_query_enrichment_config",
      |    "enabled": true,
      |    "parameters": {
      |      "inputs": [
      |        {
      |          "placeholder": 1,
      |          "pojo": {
      |            "field": "user_id"
      |          }
      |        },
      |        {
      |          "placeholder": 1,
      |          "json": {
      |            "field": "contexts",
      |            "schemaCriterion": "iglu:com.snowplowanalytics.snowplow/client_session/jsonschema/1-*-*",
      |            "jsonPath": "$.userId"
      |          }
      |        },
      |        {
      |          "placeholder": 2,
      |          "pojo": {
      |            "field": "app_id"
      |          }
      |        }
      |      ],
      |      "query": {
      |        "sql": "SELECT username, email_address, date_of_birth FROM tbl_users WHERE user = ? AND client = ? LIMIT 1"
      |       },
      |      "database": {
      |        "postgresql": {
      |          "host": "cluster01.redshift.acme.com",
      |          "port": 5439,
      |          "sslMode": true,
      |          "username": "snowplow_enrich_ro",
      |          "password": "1asIkJed",
      |          "database": "crm"
      |        }
      |      },
      |      "output": {
      |        "expectedRows": "AT_MOST_ONE",
      |        "json": {
      |          "schema": "iglu:com.acme/user/jsonschema/1-0-0",
      |          "describes": "ALL_ROWS",
      |          "propertyNames": "CAMEL_CASE"
      |        }
      |      },
      |      "cache": {
      |        "size": 3000,
      |        "ttl": 60
      |      }
      |    }
      |  }""".stripMargin)

    SqlQueryEnrichmentConfig.parse(configuration, SCHEMA_KEY) must beSuccessful(config)
  }

  def e2 = {
    // $.output.json.describes contains invalid value
    val configuration = parseJson(
      """|{
      |    "vendor": "com.snowplowanalytics.snowplow.enrichments",
      |    "name": "sql_query_enrichment_config",
      |    "enabled": true,
      |    "parameters": {
      |      "inputs": [
      |        {
      |          "placeholder": 1,
      |          "pojo": {
      |            "field": "user_id"
      |          }
      |        },
      |        {
      |          "placeholder": 1,
      |          "json": {
      |            "field": "contexts",
      |            "schemaCriterion": "iglu:com.snowplowanalytics.snowplow/client_session/jsonschema/1-*-*",
      |            "jsonPath": "$.userId"
      |          }
      |        },
      |        {
      |          "placeholder": 2,
      |          "pojo": {
      |            "field": "app_id"
      |          }
      |        }
      |      ],
      |      "query": {
      |        "sql": "SELECT username, email_address, date_of_birth FROM tbl_users WHERE user = ? AND client = ? LIMIT 1"
      |      },
      |      "database": {
      |        "postgresql": {
      |          "host": "cluster01.redshift.acme.com",
      |          "port": 5439,
      |          "sslMode": true,
      |          "username": "snowplow_enrich_ro",
      |          "password": "1asIkJed",
      |          "database": "crm"
      |        }
      |      },
      |      "output": {
      |        "expectedRows": "AT_MOST_ONE",
      |        "json": {
      |          "schema": "iglu:com.acme/user/jsonschema/1-0-0",
      |          "describes": "INVALID",
      |          "propertyNames": "CAMEL_CASE"
      |        }
      |      },
      |      "cache": {
      |        "size": 3000,
      |        "ttl": 60
      |      }
      |    }
      |  }""".stripMargin)

    SqlQueryEnrichmentConfig.parse(configuration, SCHEMA_KEY) must beFailing
  }

  def e3 = {
    val configuration = parseJson(
      """|{
      |    "vendor": "com.snowplowanalytics.snowplow.enrichments",
      |    "name": "sql_query_enrichment_config",
      |    "enabled": true,
      |    "parameters": {
      |      "inputs": [
      |        {
      |          "placeholder": 1,
      |          "pojo": {
      |            "field": "user_id"
      |          }
      |        },
      |        {
      |          "placeholder": 1,
      |          "json": {
      |            "field": "contexts",
      |            "schemaCriterion": "iglu:com.snowplowanalytics.snowplow/client_session/jsonschema/1-*-*",
      |            "jsonPath": "$.userId"
      |          }
      |        },
      |        {
      |          "placeholder": 2,
      |          "pojo": {
      |            "field": "app_id"
      |          }
      |        }
      |      ],
      |      "query": {
      |        "sql": "SELECT username, email_address, date_of_birth FROM tbl_users WHERE user = ? AND client = ? LIMIT 1"
      |       },
      |      "database": {
      |        "mysql": {
      |          "host": "cluster01.redshift.acme.com",
      |          "port": 5439,
      |          "sslMode": true,
      |          "username": "snowplow_enrich_ro",
      |          "password": "1asIkJed",
      |          "database": "crm"
      |        }
      |      },
      |      "output": {
      |        "expectedRows": "AT_LEAST_ONE",
      |        "json": {
      |          "schema": "iglu:com.acme/user/jsonschema/1-0-0",
      |          "describes": "EVERY_ROW",
      |          "propertyNames": "CAMEL_CASE"
      |        }
      |      },
      |      "cache": {
      |        "size": 3000,
      |        "ttl": 60
      |      }
      |    }
      |  }""".stripMargin)

    SqlQueryEnrichmentConfig.parse(configuration, SCHEMA_KEY) must beSuccessful
  }
}
