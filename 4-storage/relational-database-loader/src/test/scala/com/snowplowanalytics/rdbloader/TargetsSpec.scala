/*
 * Copyright (c) 2017 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0, * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
 */
package com.snowplowanalytics.rdbloader

// json4s
import org.json4s.jackson.parseJson

// specs2
import org.specs2.Specification

// Iglu client
import com.snowplowanalytics.iglu.client.Resolver

class TargetsSpec extends Specification { def is = s2"""
  Targets parse specification

    Parse Postgres storage target configuration $e1
    Parse Redshift storage target configuration $e2
    Parse Elasticsearch storage target configuration $e3
    Parse Amazon DynamoDB storage target configuration $e4

  """

  private val resolverConfig = parseJson(
    """
      |{
      |  "schema": "iglu:com.snowplowanalytics.iglu/resolver-config/jsonschema/1-0-2",
      |  "data": {
      |    "cacheSize": 500,
      |    "repositories": [
      |      {
      |        "name": "Iglu Central",
      |        "priority": 0,
      |        "vendorPrefixes": [ "com.snowplowanalytics" ],
      |        "connection": {
      |          "http": {
      |            "uri": "http://iglucentral.com"
      |          }
      |        }
      |      }
      |    ]
      |  }
      |}
    """.stripMargin)

  private val resolver = Resolver.parse(resolverConfig).toOption.get

  def e1 = {
    val config = """
      |
      |{
      |    "schema": "iglu:com.snowplowanalytics.snowplow.storage/postgresql_config/jsonschema/1-0-0",
      |    "data": {
      |        "name": "PostgreSQL enriched events storage",
      |        "host": "mydatabase.host.acme.com",
      |        "database": "ADD HERE",
      |        "port": 5432,
      |        "sslMode": "DISABLE",
      |        "username": "ADD HERE",
      |        "password": "ADD HERE",
      |        "schema": "atomic",
      |        "purpose": "ENRICHED_EVENTS"
      |    }
      |}
    """.stripMargin

    val expected = Targets.PostgresqlConfig(
      "PostgreSQL enriched events storage",
      "mydatabase.host.acme.com",
      "ADD HERE",
      5432,
      Targets.Disable,
      "atomic",
      "ADD HERE",
      "ADD HERE")

    Targets.fooo(resolver, config) must beRight(expected)
  }

  def e2 = {
    val config = """
      |{
      |    "schema": "iglu:com.snowplowanalytics.snowplow.storage/redshift_config/jsonschema/1-0-0",
      |    "data": {
      |        "name": "AWS Redshift enriched events storage",
      |        "host": "ADD HERE",
      |        "database": "ADD HERE",
      |        "port": 5439,
      |        "sslMode": "DISABLE",
      |        "username": "ADD HERE",
      |        "password": "ADD HERE",
      |        "schema": "atomic",
      |        "maxError": 1,
      |        "compRows": 20000,
      |        "purpose": "ENRICHED_EVENTS"
      |    }
      |}
    """.stripMargin

    val expected = Targets.RedshiftConfig(
      "AWS Redshift enriched events storage",
      "ADD HERE",
      "ADD HERE",
      5439,
      Targets.Disable,
      "atomic",
      "ADD HERE",
      "ADD HERE",
      1,
      20000)

    Targets.fooo(resolver, config) must beRight(expected)
  }

  def e3 = {
    val config = """
      |{
      |    "schema": "iglu:com.snowplowanalytics.snowplow.storage/elastic_config/jsonschema/1-0-0",
      |    "data": {
      |        "name": "Elasticsearch failed events storage",
      |        "host": "ADD HERE",
      |        "index": "ADD HERE",
      |        "port": 9200,
      |        "nodesWanOnly": false,
      |        "type": "ADD HERE",
      |        "purpose": "FAILED_EVENTS"
      |    }
      |}
      """.stripMargin

    val expected = Targets.ElasticConfig(
      "Elasticsearch failed events storage",
      "ADD HERE",
      "ADD HERE",
      9200,
      "ADD HERE",
      false)

    Targets.fooo(resolver, config) must beRight(expected)
  }

  def e4 = {
    val config = """
      |{
      |    "schema": "iglu:com.snowplowanalytics.snowplow.storage/amazon_dynamodb_config/jsonschema/1-0-0",
      |    "data": {
      |        "name": "AWS DynamoDB duplicates storage",
      |        "accessKeyId": "ADD HERE",
      |        "secretAccessKey": "ADD HERE",
      |        "awsRegion": "ADD HERE",
      |        "dynamodbTable": "ADD HERE",
      |        "purpose": "DUPLICATE_TRACKING"
      |    }
      |}
    """.stripMargin

    val expected = Targets.AmazonDynamodbConfig(
      "AWS DynamoDB duplicates storage",
      "ADD HERE",
      "ADD HERE",
      "ADD HERE",
      "ADD HERE")

    Targets.fooo(resolver, config) must beRight(expected)
  }
}
