/*
 * Copyright (c) 2012-2019 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.enrich.beam

import cats.Id
import com.snowplowanalytics.iglu.client.Client
import com.snowplowanalytics.iglu.core._
import com.snowplowanalytics.iglu.core.circe.CirceIgluCodecs._
import com.snowplowanalytics.snowplow.enrich.common.enrichments.EnrichmentRegistry
import io.circe.Json
import io.circe.literal._
import io.circe.syntax._

import utils._

object SpecHelpers {

  val resolverConfig = json"""
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

  val client = Client
    .parseDefault[Id](resolverConfig)
    .leftMap(_.toString)
    .value
    .fold(
      e => throw new RuntimeException(e),
      r => r
    )

  val enrichmentConfig = json"""
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

  val enrichmentsSchemaKey = SchemaKey(
    "com.snowplowanalytics.snowplow",
    "enrichments",
    "jsonschema",
    SchemaVer.Full(1, 0, 0)
  )

  val enrichmentsJson = SelfDescribingData(
    enrichmentsSchemaKey,
    Json.arr(enrichmentConfig)
  )

  val enrichmentConfs = EnrichmentRegistry
    .parse(enrichmentsJson.asJson, client, true)
    .fold(
      e => throw new RuntimeException(e.toList.mkString("\n")),
      r => r
    )

  val enrichmentRegistry = EnrichmentRegistry
    .build(enrichmentConfs)
    .fold(
      e => throw new RuntimeException(e.toList.mkString("\n")),
      r => r
    )
}
