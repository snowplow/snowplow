/*
 * Copyright (c) 2012-2016 Snowplow Analytics Ltd. All rights reserved.
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
package enrichments.registry.sqlquery

// json4s
import org.json4s._

import java.sql.Date

// specs2
import org.specs2.Specification
import org.specs2.scalaz.ValidationMatchers

class OutputSpec extends Specification with ValidationMatchers { def is =
  "This is a specification to test the Output of SQL Query Enrichment" ^
                                                                      p^
    "Parse Inetger without type hint"                       ! e1^
    "Parse Double without type hint"                        ! e2^
    "Handle null"                                           ! e3^
    "Handle java.sql.Date as ISO8601 string"                ! e4^
    end

  def e1 =
    JsonOutput.getValue(1: Integer, "") must beEqualTo(JInt(1))

  def e2 =
    JsonOutput.getValue(32.2: java.lang.Double, "") must beEqualTo(JDouble(32.2))

  def e3 =
    JsonOutput.getValue(null, "") must beEqualTo(JNull)

  def e4 = {
    val date = new Date(1465558727000L)
    JsonOutput.getValue(date, "java.sql.Date") must beEqualTo(JString("2016-06-10T11:38:47.000Z"))
  }
}
