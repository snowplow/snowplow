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

// Specs2
import org.specs2.Specification
import org.specs2.matcher.DataTables

// Scalaz
import scalaz._
import Scalaz._

/**
 * Tests the extractResolution function
 */
class ExtractResolutionTest extends Specification with DataTables {

  val FieldName = "res"
  def err: (String) => String = input => "Field [%s]: [%s] is not a valid screen resolution".format(FieldName, input)

  def is =
    "Extracting resolutions with extractResolution should work" ! e1

  def e1 =
    "SPEC NAME"        || "INPUT VAL" | "EXPECTED OUTPUT"                      |
    "valid desktop"    !! "1200x800"  !  (1200, 800).success[String]           |
    "valid mobile"     !! "76x128"    !  (76, 128).success[String]             |
    "invalid empty"    !! ""          !  err("").fail[ResolutionTuple]         |
    "invalid hex"      !! "76xEE"     !  err("76xEE").fail[ResolutionTuple]    |
    "invalid negative" !! "1200x-17"  !  err("1200x-17").fail[ResolutionTuple] |> {

      (_, input, expected) => ClientEnrichments.extractResolution(FieldName, input) must_== expected
    }
}
