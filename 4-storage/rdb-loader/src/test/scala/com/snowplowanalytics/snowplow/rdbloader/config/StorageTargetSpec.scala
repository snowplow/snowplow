/*
 * Copyright (c) 2012-2017 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0, * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
 */
package com.snowplowanalytics.snowplow.rdbloader
package config

// json4s
import org.json4s.jackson.parseJson

// specs2
import org.specs2.Specification

// Iglu client
import com.snowplowanalytics.iglu.client.Resolver

class StorageTargetSpec extends Specification { def is = s2"""
  Parse Postgres storage target configuration $e1
  Parse Redshift storage target configuration $e2
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
  private val parseWithDefaultResolver = StorageTarget.parseTarget(resolver, _: String)

  def e1 = {
    val config = """
      |
      |{
      |    "schema": "iglu:com.snowplowanalytics.snowplow.storage/postgresql_config/jsonschema/1-0-1",
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

    val expected = StorageTarget.PostgresqlConfig(
      None,
      "PostgreSQL enriched events storage",
      "mydatabase.host.acme.com",
      "ADD HERE",
      5432,
      StorageTarget.Disable,
      "atomic",
      "ADD HERE",
      "ADD HERE")

    parseWithDefaultResolver(config).toEither must beRight(expected)
  }

  def e2 = {
    val config = """
      |{
      |    "schema": "iglu:com.snowplowanalytics.snowplow.storage/redshift_config/jsonschema/2-0-0",
      |    "data": {
      |        "name": "AWS Redshift enriched events storage",
      |        "host": "ADD HERE",
      |        "database": "ADD HERE",
      |        "port": 5439,
      |        "sslMode": "DISABLE",
      |        "username": "ADD HERE",
      |        "password": "ADD HERE",
      |        "roleArn": "arn:aws:iam::123456789876:role/RedshiftLoadRole",
      |        "schema": "atomic",
      |        "maxError": 1,
      |        "compRows": 20000,
      |        "purpose": "ENRICHED_EVENTS"
      |    }
      |}
    """.stripMargin

    val expected = StorageTarget.RedshiftConfig(
      None,
      "AWS Redshift enriched events storage",
      "ADD HERE",
      "ADD HERE",
      5439,
      StorageTarget.Disable,
      "arn:aws:iam::123456789876:role/RedshiftLoadRol" +
        "e",
      "atomic",
      "ADD HERE",
      "ADD HERE",
      1,
      20000)

    parseWithDefaultResolver(config).toEither must beRight(expected)
  }
}
