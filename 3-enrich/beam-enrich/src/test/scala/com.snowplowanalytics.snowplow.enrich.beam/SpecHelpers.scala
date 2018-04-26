/*
 * Copyright (c) 2012-2018 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at
 * http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and
 * limitations there under.
 */
package com.snowplowanalytics
package snowplow
package enrich
package beam

import scalaz._
import Scalaz._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

import common.enrichments.EnrichmentRegistry
import common.utils.JsonUtils
import iglu.client.Resolver

object SpecHelpers {

  val resolverConfig = """
    {
      "schema": "iglu:com.snowplowanalytics.iglu/resolver-config/jsonschema/1-0-2",
      "data": {
        "cacheSize": 500,
        "repositories": [
          {
            "name": "Iglu Central",
            "priority": 0,
            "vendorPrefixes": [ "com.snowplowanalytics" ],
            "connection": { "http": { "uri": "http://iglucentral.com" } }
          }
        ]
      }
    }
  """

  implicit val resolver = (for {
    json <- JsonUtils.extractJson("", resolverConfig)
    resolver <- Resolver.parse(json).leftMap(_.toString)
  } yield resolver).fold(
    e => throw new RuntimeException(e),
    r => r
  )

  val enrichmentConfig = """
    {
      "schema": "iglu:com.snowplowanalytics.snowplow/anon_ip/jsonschema/1-0-0",
      "data": {
        "name": "anon_ip",
        "vendor": "com.snowplowanalytics.snowplow",
        "enabled": true,
        "parameters": { "anonOctets": 1 }
      }
    }
  """

  val enrichmentRegistry = (for {
    combinedJson <-
      (("schema" -> "iglu:com.snowplowanalytics.snowplow/enrichments/jsonschema/1-0-0") ~
      ("data" -> List(parse(enrichmentConfig)))).success
    registry <- EnrichmentRegistry.parse(combinedJson, false).leftMap(_.toList.mkString("\n"))
  } yield registry).fold(
    e => throw new RuntimeException(e),
    r => r
  )

}
