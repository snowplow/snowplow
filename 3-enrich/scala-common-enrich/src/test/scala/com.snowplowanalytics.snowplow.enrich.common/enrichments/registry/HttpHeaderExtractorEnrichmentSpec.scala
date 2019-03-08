/**Copyright (c) 2012-2019 Snowplow Analytics Ltd. All rights reserved.
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

import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.specs2.Specification
import org.specs2.scalaz._

class HttpHeaderExtractorEnrichmentSpec extends Specification with ValidationMatchers {
  def is = s2"""
  This is a specification to test the HttpHeaderExtractorEnrichment
  returns X-Forwarded-For header             $e1
  returns Accept header after Regex matching $e2
  No headers                                 $e3
  """

  def e1 = {
    val expected = List(
      """{"schema":"iglu:org.ietf/http_header/jsonschema/1-0-0","data":{"name":"X-Forwarded-For","value":"129.78.138.66, 129.78.64.103"}}"""
    )

    HttpHeaderExtractorEnrichment("X-Forwarded-For")
      .extract(List("X-Forwarded-For: 129.78.138.66, 129.78.64.103"))
      .map(h => compact(render(h))) must_== expected.map(e => compact(render(parse(e))))
  }

  def e2 = {
    val expected = List(
      """{"schema":"iglu:org.ietf/http_header/jsonschema/1-0-0","data":{"name":"Accept","value":"text/html"}}"""
    )

    HttpHeaderExtractorEnrichment(".*").extract(List("Accept: text/html")).map(h => compact(render(h))) must_== expected
      .map(e                                                                     => compact(render(parse(e))))
  }

  def e3 = {
    val expected = List.empty[String]

    HttpHeaderExtractorEnrichment(".*")
      .extract(Nil)
      .map(h => compact(render(h))) must_== expected.map(e => compact(render(parse(e))))
  }
}
