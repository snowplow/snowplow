/*
 * Copyright (c) 2012-2013 SnowPlow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.hadoop.etl
package enrichments
package web

// Specs2
import org.specs2.Specification
import org.specs2.matcher.DataTables

// Scalaz
import scalaz._
import Scalaz._

/**
 * Tests the extractMarketingFields function.
 * Uses DataTables.
 */
class ExtractMarketingFieldsTest extends Specification with DataTables {

  val Encoding = "UTF-8"

  def is =
    "Extracting marketing campaigns with extractMarketingFields should work" ! e1

  // TODO: write this!!!
  // Start with EXPECTED fields, then URL
  def e1 =
    "SPEC NAME"                      || "EXPECTED MEDIUM" | "EXPECTED SOURCE" |
    "valid web"                      !! "web"       ! "web".success     |
    "valid iot (internet of things)" !! "iot"       ! "iot".success     |
    "invalid empty"                  !! ""          !  "".fail     |
    "invalid platform"               !! "ma"        !  "ma".fail   |> {

      (_, url, expected) => 1 must_== 1
    }
}