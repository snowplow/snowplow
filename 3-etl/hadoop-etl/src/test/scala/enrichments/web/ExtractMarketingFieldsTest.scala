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
package com.snowplowanalytics.snowplow.enrich.hadoop
package enrichments
package web

// Java
import java.net.URI

// Specs2 & Scalaz-Specs2
import org.specs2.Specification
import org.specs2.matcher.DataTables
import org.specs2.scalaz.ValidationMatchers

// Scalaz
import scalaz._
import Scalaz._

// SnowPlow Utils
import com.snowplowanalytics.util.Tap._

/**
 * Tests the extractMarketingFields function.
 * Uses DataTables.
 */
class ExtractMarketingFieldsTest extends Specification with DataTables with ValidationMatchers {

  val Encoding = "UTF-8"

  def is =
    "Extracting valid marketing campaigns with extractMarketingFields should work" ! e1

  // TODO: add in some invalid URLs etc.

  // Valid marketing campaigns
  // Use http://support.google.com/analytics/bin/answer.py?hl=en&answer=1033867 to generate additional ones
  def e1 =
    "SPEC NAME"                      || "EXP. SOURCE" | "EXP. MEDIUM" | "EXP. TERM"      | "EXP. CONTENT" | "EXP. CAMPAIGN" | "URL" |
    "all except content"             !! "google"      ! "cpc"         ! "buy tarot"      ! null           ! "spring_sale"   ! new URI("http://www.psychicbazaar.com/shop/tarot?utm_source=google&utm_medium=cpc&utm_term=buy%2Btarot&utm_campaign=spring_sale") |
    "just source, medium & campaign" !! "newsletter4" ! "email"       ! null             ! null           ! "slogan"        ! new URI("http://www.example.com/?utm_source=newsletter4&utm_medium=email&utm_campaign=slogan") |
    "all"                            !! "citysearch"  ! "banner"      ! "cola fizzy pop" ! "creative 1"   ! "promo code"    ! new URI("http://www.example.com/?utm_source=citysearch&utm_medium=banner&utm_term=cola%2Bfizzy%2Bpop&utm_content=creative%2B1&utm_campaign=promo%2Bcode") |> {

      (_, source, medium, term, content, campaign, url) =>
        val expected = new AttributionEnrichments.MarketingCampaign().tap { mc =>
          mc.source   = source
          mc.medium   = medium
          mc.term     = term
          mc.content  = content
          mc.campaign = campaign 
        }
        AttributionEnrichments.extractMarketingFields(url, Encoding) must beSuccessful(expected)
    }
}