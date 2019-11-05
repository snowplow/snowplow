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
package enrichments

import cats.syntax.either._
import com.snowplowanalytics.snowplow.badrows._
import org.specs2.Specification
import org.specs2.matcher.DataTables

class ExtractViewDimensionsSpec extends Specification with DataTables {

  val FieldName = "res"
  def err: String => FailureDetails.EnrichmentFailure =
    input =>
      FailureDetails.EnrichmentFailure(
        None,
        FailureDetails.EnrichmentFailureMessage.InputData(
          FieldName,
          Option(input),
          """does not conform to regex (\d+)x(\d+)"""
        )
      )
  def err2: String => FailureDetails.EnrichmentFailure =
    input =>
      FailureDetails.EnrichmentFailure(
        None,
        FailureDetails.EnrichmentFailureMessage.InputData(
          FieldName,
          Option(input),
          "could not be converted to java.lang.Integer s"
        )
      )

  def is = s2"""
  Extracting screen dimensions (viewports, screen resolution etc) with extractViewDimensions should work $e1"""

  def e1 =
    "SPEC NAME" || "INPUT VAL" | "EXPECTED OUTPUT" |
      "valid desktop" !! "1200x800" ! (1200, 800).asRight |
      "valid mobile" !! "76x128" ! (76, 128).asRight |
      "invalid empty" !! "" ! err("").asLeft |
      "invalid null" !! null ! err(null).asLeft |
      "invalid hex" !! "76xEE" ! err("76xEE").asLeft |
      "invalid negative" !! "1200x-17" ! err("1200x-17").asLeft |
      "Arabic number" !! "٤٥٦٧x680" ! err("٤٥٦٧x680").asLeft |
      "number > int #1" !! "760x3389336768" ! err2("760x3389336768").asLeft |
      "number > int #2" !! "9989336768x1200" ! err2("9989336768x1200").asLeft |> {
      (_, input, expected) =>
        ClientEnrichments.extractViewDimensions(FieldName, input) must_== expected
    }
}
