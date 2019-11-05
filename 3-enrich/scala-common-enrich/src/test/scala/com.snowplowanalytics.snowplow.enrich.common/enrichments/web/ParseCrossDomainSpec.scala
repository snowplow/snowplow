/*
 * Copyright (c) 2012-2019 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.enrich.common
package enrichments.web

import cats.syntax.option._
import com.snowplowanalytics.snowplow.badrows._
import org.specs2.Specification
import org.specs2.matcher.DataTables

class ParseCrossDomainSpec extends Specification with DataTables {
  def is = s2"""
  parseCrossDomain should return None when the querystring contains no _sp parameter           $e1
  parseCrossDomain should return a failure when the _sp timestamp is unparseable               $e2
  parseCrossDomain should successfully extract the domain user ID when available               $e3
  parseCrossDomain should successfully extract the domain user ID and timestamp when available $e4
  parseCrossDomain should extract neither field from an empty _sp parameter                    $e5
  """
  def e1 =
    PageEnrichments.parseCrossDomain(Map()) must beRight((None, None))

  def e2 = {
    val expected = FailureDetails.EnrichmentFailure(
      None,
      FailureDetails.EnrichmentFailureMessage.InputData(
        "sp_dtm",
        "not-a-timestamp".some,
        "not in the expected format: ms since epoch"
      )
    )
    PageEnrichments.parseCrossDomain(Map("_sp" -> "abc.not-a-timestamp")) must beLeft(expected)
  }

  def e3 =
    PageEnrichments.parseCrossDomain(Map("_sp" -> "abc")) must beRight(("abc".some, None))

  def e4 =
    PageEnrichments.parseCrossDomain(Map("_sp" -> "abc.1426245561368")) must beRight(
      ("abc".some, "2015-03-13 11:19:21.368".some)
    )

  def e5 =
    PageEnrichments.parseCrossDomain(Map("_sp" -> "")) must beRight(None -> None)
}
