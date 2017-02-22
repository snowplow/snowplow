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

// cats
import cats.syntax.either._

// circe
import io.circe._
import io.circe.yaml.parser

// specs2
import org.specs2.Specification

// This project
import Config.Codecs._

class TargetsSpec extends Specification { def is = s2"""
  Targets parse specification

    Parse Postgres storage target configuration $e1

  """

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



  }
}
