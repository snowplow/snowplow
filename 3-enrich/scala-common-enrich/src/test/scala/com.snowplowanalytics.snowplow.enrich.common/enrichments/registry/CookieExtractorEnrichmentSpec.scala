/**Copyright (c) 2012-2020 Snowplow Analytics Ltd. All rights reserved.
| *
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

import io.circe.literal._

import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer, SelfDescribingData}

import org.specs2.Specification

class CookieExtractorEnrichmentSpec extends Specification {
  def is = s2"""
  returns an empty list when no cookie header                $e1
  returns an empty list when no cookie matches configuration $e2
  returns contexts for found cookies                         $e3
  """

  def e1 = {
    val actual = CookieExtractorEnrichment(List("cookieKey1")).extract(List("Content-Length: 348"))
    actual must_== Nil
  }

  def e2 = {
    val actual = CookieExtractorEnrichment(List("cookieKey1"))
      .extract(List("Cookie: not-interesting-cookie=1234;"))
    actual must_== Nil
  }

  def e3 = {
    val cookies = List("ck1", "=cv2", "ck3=", "ck4=cv4", "ck5=\"cv5\"")
    val cookieKeys = List("ck1", "", "ck3", "ck4", "ck5")

    val expected =
      List(
        SelfDescribingData(
          SchemaKey("org.ietf", "http_cookie", "jsonschema", SchemaVer.Full(1, 0, 0)),
          json"""{"name":"ck1","value":null}"""
        ),
        SelfDescribingData(
          SchemaKey("org.ietf", "http_cookie", "jsonschema", SchemaVer.Full(1, 0, 0)),
          json"""{"name":"","value":"cv2"}"""
        ),
        SelfDescribingData(
          SchemaKey("org.ietf", "http_cookie", "jsonschema", SchemaVer.Full(1, 0, 0)),
          json"""{"name":"ck3","value":""}"""
        ),
        SelfDescribingData(
          SchemaKey("org.ietf", "http_cookie", "jsonschema", SchemaVer.Full(1, 0, 0)),
          json"""{"name":"ck4","value":"cv4"}"""
        ),
        SelfDescribingData(
          SchemaKey("org.ietf", "http_cookie", "jsonschema", SchemaVer.Full(1, 0, 0)),
          json"""{"name":"ck5","value":"cv5"}"""
        )
      )

    val actual = CookieExtractorEnrichment(cookieKeys)
      .extract(List("Cookie: " + cookies.mkString(";")))

    actual must beLike {
      case cookies @ _ :: _ :: _ :: _ :: _ :: Nil => cookies must_== expected
    }
  }
}
