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

class ExtractPageUriTest extends Specification with DataTables with ValidationMatchers { def is =

  "This is a specification to test the extractPageUri function"                                    ^
                                                                                                  p^
  "extractPageUri should return failure when it has no page URI provided"                          ! e1^
  "extractPageUri should choose the most complete page URI when it has one or two to choose from"  ! e2^
                                                                                                   end

  // No URI
  def e1 =
    PageEnrichments.extractPageUri(None, None) must beFailing("No page URI provided")

  // Valid URI combinations
  val fullUri = "http://www.psychicbazaar.com/2-tarot-cards/genre/all/type/all?p=2&n=48"
  val truncatedUri = fullUri.take(fullUri.length - 5)
  val fullURI = new URI(fullUri)

  def e2 =
    "SPEC NAME"                                     || "URI TAKEN FROM COLLECTOR'S REFERER" | "URI SENT BY TRACKER" | "EXPECTED URI" |
    "both URIs match (98% of the time)"             !! fullUri.some                         ! fullUri.some          ! fullURI        |
    "tracker didn't send URI (e.g. No-JS Tracker)"  !! fullUri.some                         ! None                  ! fullURI        |
    "collector didn't record the referer (rare)"    !! None                                 ! fullUri.some          ! fullURI        |
    "tracker truncated URI (IE might do this)"      !! fullUri.some                         ! truncatedUri.some     ! fullURI        |
    "collector truncated URI (should never happen)" !! truncatedUri.some                    ! fullUri.some          ! fullURI        |> {
      (_, fromReferer, fromTracker, expected) =>
        PageEnrichments.extractPageUri(fromReferer, fromTracker) must beSuccessful(expected)
    }
}