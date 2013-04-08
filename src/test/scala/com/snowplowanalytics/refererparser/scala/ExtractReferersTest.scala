/**
 * Copyright 2012-2013 Snowplow Analytics Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.snowplowanalytics.refererparser.scala

// Java
import java.net.URI

// Specs2
import org.specs2.Specification
import org.specs2.matcher.DataTables

class ExtractReferersTest extends Specification with DataTables { def is =

  "This is a specification to test the parse function"                                             ^
                                                                                                  p^
  "parse should successfully extract referer details from URIs with recognised referers"           ! e1^
  "parse should work the same regardless of which arguments are used to call it"                   ! e2^
  "parse should return None when the provided referer URI is null or an empty String"              ! e3^
  "parse should return unknown when the provided referer URI is not recognised"                    ! e4^
                                                                                                   end

  // Aliases
  val pageUri = "www.snowplowanalytics.com"

  // Successful extractions
  def e1 =
    "SPEC NAME"            || "REFERER URI"                                                                                  | "REFERER MEDIUM" | "REFERER SOURCE"    | "REFERER TERM"                           |
    "Google search"        !! "http://www.google.com/search?q=gateway+oracle+cards+denise+linn&hl=en&client=safari"          ! Medium.Search    ! Some("Google")      ! Some("gateway oracle cards denise linn") |
    "Yahoo! search"        !! "http://es.search.yahoo.com/search;_ylt=A7x9QbwbZXxQ9EMAPCKT.Qt.?p=BIEDERMEIER+FORTUNE+TELLING+CARDS&ei=utf-8&type=685749&fr=chr-greentree_gc&xargs=0&pstart=1&b=11" ! Medium.Search ! Some("Yahoo!") ! Some("BIEDERMEIER FORTUNE TELLING CARDS") |
    "PriceRunner search"   !! "http://www.pricerunner.co.uk/search?displayNoHitsMessage=1&q=wild+wisdom+of+the+faery+oracle" ! Medium.Search    ! Some("PriceRunner") ! Some("wild wisdom of the faery oracle")  |
    "AOL search"           !! "http://aolsearch.aol.co.uk/aol/search?s_chn=hp&enabled_terms=&s_it=aoluk-homePage50&q=pendulums" ! Medium.Search ! Some("AOL")         ! Some("pendulums")                        |
    "Twitter redirect"     !! "http://t.co/wzJw3zr"                                                                          ! Medium.Social    ! Some("Twitter")     ! None                                     |
    "Facebook social"      !! "http://www.facebook.com/l.php?u=http%3A%2F%2Fwww.psychicbazaar.com&h=yAQHZtXxS&s=1"           ! Medium.Social    ! Some("Facebook")    ! None                                     |
    "Tumblr social #1"     !! "http://www.tumblr.com/dashboard"                                                              ! Medium.Social    ! Some("Tumblr")      ! None                                     |
    "Tumblr w subdomain"   !! "http://psychicbazaar.tumblr.com/"                                                             ! Medium.Social    ! Some("Tumblr")      ! None                                     |
    "Yahoo! Mail"          !! "http://36ohk6dgmcd1n-c.c.yom.mail.yahoo.net/om/api/1.0/openmail.app.invoke/36ohk6dgmcd1n/11/1.0.35/us/en-US/view.html/0" ! Medium.Email ! Some("Yahoo! Mail") ! None              |
    "Internal referal #1"  !! "https://www.snowplowanalytics.com/shop/oracles"                                               ! Medium.Internal  ! None                ! None                                     |> {
      (_, refererUri, medium, source, term) =>
        Parser.parse(refererUri, pageUri) must_== Some(Referer(medium, source, term))
    }

  // Different API calls TODO
  def e2 =
    1 must_== 1

  // Null/empty referer URI TODO
  def e3 =
    1 must_== 1

  // Unknown referer URI TODO
  def e4 =
    1 must_== 1
}