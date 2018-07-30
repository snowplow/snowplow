/**Copyright (c) 2012-2018 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.enrich.common
package enrichments
package registry

// Specs2
import org.specs2.Specification
import org.specs2.scalaz._

// Scalaz
import scalaz._
import Scalaz._

// Json4s
import org.json4s._
import org.json4s.JValue
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

class CookieExtractorEnrichmentSpec extends Specification with ValidationMatchers {
  def is = s2"""
  This is a specification to test the CookieExtractorEnrichment
  returns an empty list when no cookie header                $e1
  returns an empty list when no cookie matches configuration $e2
  returns contexts for found cookies                         $e3
  """

  def e1 = {
    val actual = CookieExtractorEnrichment(List("cookieKey1")).extract(List("Content-Length: 348"))

    actual must_== Nil
  }

  def e2 = {
    val actual = CookieExtractorEnrichment(List("cookieKey1")).extract(List("Cookie: not-interesting-cookie=1234;"))

    actual must_== Nil
  }

  def e3 = {
    val cookies    = List("ck1", "=cv2", "ck3=", "ck4=cv4", "ck5=\"cv5\"")
    val cookieKeys = List("ck1", "", "ck3", "ck4", "ck5")

    val expected = List(
      """{"schema":"iglu:org.ietf/http_cookie/jsonschema/1-0-0","data":{"name":"ck1","value":null}}""",
      """{"schema":"iglu:org.ietf/http_cookie/jsonschema/1-0-0","data":{"name":"","value":"cv2"}}""",
      """{"schema":"iglu:org.ietf/http_cookie/jsonschema/1-0-0","data":{"name":"ck3","value":""}}""",
      """{"schema":"iglu:org.ietf/http_cookie/jsonschema/1-0-0","data":{"name":"ck4","value":"cv4"}}""",
      """{"schema":"iglu:org.ietf/http_cookie/jsonschema/1-0-0","data":{"name":"ck5","value":"cv5"}}"""
    )

    val actual = CookieExtractorEnrichment(cookieKeys).extract(List("Cookie: " + cookies.mkString(";")))

    actual must beLike {
      case cookies @ _ :: _ :: _ :: _ :: _ :: Nil => {
        cookies.map(c => compact(render(c))) must_== expected.map(e => compact(render(parse(e))))
      }
    }
  }
}
