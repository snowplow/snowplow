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

class ExtractPageUriSpec extends Specification with DataTables with ValidationMatchers { def is =

  "This is a specification to test the extractPageUri function"                                ^
                                                                                              p^
  "extractPageUri should return a None when no page URI provided"                              ! e1^
  "extractPageUri should choose the URI from the tracker if it has one or two to choose from"  ! e2^
  "extractPageUri will alas assume a browser-truncated page URL is a custom URL not an error"  ! e3^
                                                                                               end

  // No URI
  def e1 =
    PageEnrichments.extractPageUri(None, None) must beSuccessful(None)

  // Valid URI combinations
  val originalUri = "http://www.mysite.com/shop/session/_internal/checkout"
  val customUri = "http://www.mysite.com/shop/checkout" // E.g. set by setCustomUrl in JS Tracker
  val originalURI = new URI(originalUri)
  val customURI = new URI(customUri)

  def e2 =
    "SPEC NAME"                                       || "URI TAKEN FROM COLLECTOR'S REFERER" | "URI SENT BY TRACKER" | "EXPECTED URI"   |
    "both URIs match (98% of the time)"               !! originalUri.some                     ! originalUri.some      ! originalURI.some |
    "tracker didn't send URI (e.g. No-JS Tracker)"    !! originalUri.some                     ! None                  ! originalURI.some |
    "collector didn't record the referer (rare)"      !! None                                 ! originalUri.some      ! originalURI.some |
    "collector and tracker URIs differ - use tracker" !! originalUri.some                     ! customUri.some        ! customURI.some   |> {
      
      (_, fromReferer, fromTracker, expected) =>
        PageEnrichments.extractPageUri(fromReferer, fromTracker) must beSuccessful(expected)
    }

  // Truncate
  val truncatedUri = originalUri.take(originalUri.length - 5)
  val truncatedURI = new URI(truncatedUri)

  // See https://github.com/snowplow/snowplow/issues/268 for background behind this test
  def e3 =
    PageEnrichments.extractPageUri(originalUri.some, truncatedUri.some) must beSuccessful(truncatedURI.some)
}