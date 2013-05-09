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

// referer-parser
import com.snowplowanalytics.refererparser.scala.{Medium, Referer}

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

/**
 * A small selection of tests borrowed from referer-parser.
 *
 * This is a very imcomplete set - more a tripwire than an exhaustive test.
 * Please see referer-parser's test suite for the full set of tests:
 *
 * https://github.com/snowplow/referer-parser/tree/master/java-scala/src/test/scala/com/snowplowanalytics/refererparser/scala
 */
class ExtractRefererDetailsTest extends Specification with DataTables { def is =

  "This is a specification to test extractRefererDetails"              ^
                                                                      p^
    "Parsing referer URIs should work"                                 ! e1^
    "Tabs and newlines in search terms should be replaced"             ! e2^
                                                                       end

  val PageHost = "www.snowplowanalytics.com"

  def e1 =
    "SPEC NAME"        || "REFERER URI"                                                                                                             | "REFERER MEDIUM" | "REFERER SOURCE"    | "REFERER TERM"                           |
    "Google search"    !! "http://www.google.com/search?q=gateway+oracle+cards+denise+linn&hl=en&client=safari"                                     ! Medium.Search    ! Some("Google")      ! Some("gateway oracle cards denise linn") |
    "Facebook social"  !! "http://www.facebook.com/l.php?u=http%3A%2F%2Fwww.psychicbazaar.com&h=yAQHZtXxS&s=1"                                      ! Medium.Social    ! Some("Facebook")    ! None                                     |
    "Yahoo! Mail"      !! "http://36ohk6dgmcd1n-c.c.yom.mail.yahoo.net/om/api/1.0/openmail.app.invoke/36ohk6dgmcd1n/11/1.0.35/us/en-US/view.html/0" ! Medium.Email     ! Some("Yahoo! Mail") ! None                                     |
    "Internal referer" !! "https://www.snowplowanalytics.com/account/profile"                                                                       ! Medium.Internal  ! None                ! None                                     |
    "Unknown referer"  !! "http://www.spyfu.com/domain.aspx?d=3897225171967988459"                                                                  ! Medium.Unknown   ! None                ! None                                     |> {                                                                                                                   
      (_, refererUri, medium, source, term) =>
        AttributionEnrichments.extractRefererDetails(new URI(refererUri), PageHost) must_== Some(Referer(medium, source, term))
    }

  def e2 =
    AttributionEnrichments.extractRefererDetails(new URI("http://www.google.com/search?q=%0Agateway%09oracle%09cards%09denise%09linn&hl=en&client=safari"), PageHost) must_== Some(Referer(Medium.Search, Some("Google"), Some("gateway    oracle    cards    denise    linn"))) 
}