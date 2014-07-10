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
class ExtractViewDimensionsTest extends Specification with DataTables {

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

class UserAgentParseTest extends org.specs2.mutable.Specification with ValidationMatchers with DataTables {
  import ClientEnrichments._

  "useragent parser" should {
    "parse useragent" in {
      "Input UserAgent" | "Parsed UserAgent" |
      "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/33.0.1750.152 Safari/537.36" !! ClientAttributes(browserName = "Chrome 33", browserFamily="Chrome", browserVersion = Some("33.0.1750.152"), browserType = "Browser", browserRenderEngine = "WEBKIT", osName = "Mac OS X", osFamily = "Mac OS X", osManufacturer = "Apple Inc.", deviceType = "Computer", deviceIsMobile = false) |
      "Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; rv:11.0" !! ClientAttributes(browserName = "Internet Explorer 11", browserFamily="Internet Explorer", browserVersion = Some("11.0"), browserType = "Browser", browserRenderEngine = "TRIDENT", osName = "Windows 7", osFamily = "Windows", osManufacturer = "Microsoft Corporation", deviceType = "Computer", deviceIsMobile = false) |> {
        (input, expected) => {
          ClientEnrichments.extractClientAttributes(input) must beSuccessful.like { case a => a must_== expected }
        }
      }
    }
  }
}
