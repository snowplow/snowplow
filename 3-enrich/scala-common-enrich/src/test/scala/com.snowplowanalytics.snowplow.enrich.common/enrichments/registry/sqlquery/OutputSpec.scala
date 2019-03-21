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
package com.snowplowanalytics.snowplow.enrich.common.enrichments.registry.sqlquery

import java.sql.Date

import io.circe._
import org.joda.time.DateTime
import org.specs2.Specification

class OutputSpec extends Specification {
  def is = s2"""
  This is a specification to test the Output of SQL Query Enrichment
  Parse Integer without type hint        $e1
  Parse Double without type hint         $e2
  Handle null                            $e3
  Handle java.sql.Date as ISO8601 string $e4
  """

  def e1 =
    JsonOutput.getValue(1: Integer, "") must beEqualTo(Json.fromInt(1))

  def e2 =
    JsonOutput.getValue(32.2: java.lang.Double, "") must beEqualTo(Json.fromDoubleOrNull(32.2))

  def e3 =
    JsonOutput.getValue(null, "") must beEqualTo(Json.Null)

  def e4 = {
    val date = new Date(1465558727000L)
    JsonOutput.getValue(date, "java.sql.Date") must
      beEqualTo(Json.fromString(new DateTime(date).toString))
  }
}
