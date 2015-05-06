/*
 * Copyright (c) 2012-2014 Snowplow Analytics Ltd. All rights reserved.
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
package enrichments

// Specs2
import org.specs2.Specification
import org.specs2.matcher.DataTables
import org.specs2.scalaz._

// Scalaz
import scalaz._
import Scalaz._

/**
 * Tests the extractViewDimensions function
 */
class ExtractViewDimensionsSpec extends Specification with DataTables {

  val FieldName = "res"
  def err: (String) => String = input => "Field [%s]: [%s] does not contain valid view dimensions".format(FieldName, input)
  def err2: (String) => String = input => "Field [%s]: view dimensions [%s] exceed Integer's max range".format(FieldName, input)

  def is =
    "Extracting screen dimensions (viewports, screen resolution etc) with extractViewDimensions should work" ! e1

  def e1 =
    "SPEC NAME"        || "INPUT VAL"       | "EXPECTED OUTPUT"            |
    "valid desktop"    !! "1200x800"        ! (1200, 800).success          |
    "valid mobile"     !! "76x128"          ! (76, 128).success            |
    "invalid empty"    !! ""                ! err("").fail                 |
    "invalid null"     !! null              ! err(null).fail               |
    "invalid hex"      !! "76xEE"           ! err("76xEE").fail            |
    "invalid negative" !! "1200x-17"        ! err("1200x-17").fail         |
    "Arabic number"    !! "٤٥٦٧x680"        ! err("٤٥٦٧x680").fail         |
    "number > int #1"  !! "760x3389336768"  ! err2("760x3389336768").fail  |
    "number > int #2"  !! "9989336768x1200" ! err2("9989336768x1200").fail |> {

      (_, input, expected) => ClientEnrichments.extractViewDimensions(FieldName, input) must_== expected
    }
}
