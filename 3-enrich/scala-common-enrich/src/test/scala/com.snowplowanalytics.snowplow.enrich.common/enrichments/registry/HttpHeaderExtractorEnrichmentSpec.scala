/**Copyright (c) 2012-2020 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.enrich.common.enrichments.registry

import io.circe._
import io.circe.literal._

import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer, SelfDescribingData}

import org.specs2.Specification

class HttpHeaderExtractorEnrichmentSpec extends Specification {
  def is = s2"""
  returns X-Forwarded-For header             $e1
  returns Accept header after Regex matching $e2
  no headers                                 $e3
  """

  def e1 = {
    val expected = List(
      SelfDescribingData(
        SchemaKey("org.ietf", "http_header", "jsonschema", SchemaVer.Full(1, 0, 0)),
        json"""{"name":"X-Forwarded-For","value":"129.78.138.66, 129.78.64.103"}"""
      )
    )
    HttpHeaderExtractorEnrichment("X-Forwarded-For")
      .extract(List("X-Forwarded-For: 129.78.138.66, 129.78.64.103")) must_== expected
  }

  def e2 = {
    val expected = List(
      SelfDescribingData(
        SchemaKey("org.ietf", "http_header", "jsonschema", SchemaVer.Full(1, 0, 0)),
        json"""{"name":"Accept","value":"text/html"}"""
      )
    )
    HttpHeaderExtractorEnrichment(".*").extract(List("Accept: text/html")) must_== expected
  }

  def e3 = {
    val expected = List.empty[Json]
    HttpHeaderExtractorEnrichment(".*").extract(Nil) must_== expected
  }
}
