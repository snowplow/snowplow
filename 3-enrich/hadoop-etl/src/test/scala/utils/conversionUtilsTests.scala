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
package utils

// Java
import java.net.URI

// Specs2
import org.specs2.Specification
import org.specs2.matcher.DataTables
import org.specs2.scalaz.ValidationMatchers

/**
 * Tests the explodeUri function
 */
class ExplodeUriTest extends Specification with DataTables {

  def is =
    "Exploding URIs into their component pieces with explodeUri should work" ! e1

  def e1 =
    "SPEC NAME"               || "URI"                                                                           | "EXP. SCHEME" | "EXP. HOST"             | "EXP. PORT" | "EXP. PATH"                             | "EXP. QUERY"       | "EXP. FRAGMENT" |
    "With path, qs & #"       !! "http://www.psychicbazaar.com/oracles/119-psycards-deck.html?view=print#detail" ! "http"        ! "www.psychicbazaar.com" ! 80          ! Some("/oracles/119-psycards-deck.html") ! Some("view=print") ! Some("detail")  |
    "With path & space in qs" !! "http://psy.bz/genre/all/type/all?utm_source=google&utm_medium=cpc&utm_term=buy%2Btarot&utm_campaign=spring_sale" ! "http" ! "psy.bz" ! 80  ! Some("/genre/all/type/all") ! Some("utm_source=google&utm_medium=cpc&utm_term=buy+tarot&utm_campaign=spring_sale") ! None |
    "With path & no www"      !! "http://snowplowanalytics.com/analytics/index.html"                             ! "http"        ! "snowplowanalytics.com" ! 80          ! Some("/analytics/index.html")           ! None               ! None            |
    "Port specified"          !! "http://www.nbnz.co.nz:440/login.asp"                                           ! "http"        ! "www.nbnz.co.nz"        ! 440         ! Some("/login.asp")                      ! None               ! None            |
    "HTTPS & #"               !! "https://www.lancs.ac.uk#footer"                                                ! "https"       ! "www.lancs.ac.uk"       ! 80          ! None                                    ! None               ! Some("footer")  |
    "www2 & trailing /"       !! "https://www2.williamhill.com/"                                                 ! "https"       ! "www2.williamhill.com"  ! 80          ! Some("/")                               ! None               ! None            |
    "Tab & newline in qs"     !! "http://www.ebay.co.uk/sch/i.html?_from=R40&_trksid=m570.l2736&_nkw=%09+Clear+Quartz+Point+Rock+Crystal%0ADowsing+Pendulum" ! "http" ! "www.ebay.co.uk" ! 80 ! Some("/sch/i.html") ! Some("_from=R40&_trksid=m570.l2736&_nkw=    +Clear+Quartz+Point+Rock+CrystalDowsing+Pendulum") ! None |
    "Tab & newline in path"   !! "https://snowplowanalytics.com/analytic%0As/index%09nasty.html"                 ! "https"       ! "snowplowanalytics.com" ! 80          ! Some("/analytics/index    nasty.html")  ! None               ! None            |
    "Tab & newline in #"      !! "http://psy.bz/oracles/psycards.html?view=print#detail%09is%0Acorrupted"        ! "http"        ! "psy.bz"                ! 80          ! Some("/oracles/psycards.html")          ! Some("view=print") ! Some("detail    iscorrupted") |> {

      (_, uri, scheme, host, port, path, query, fragment) => {
        val actual = ConversionUtils.explodeUri(new URI(uri))
        val expected = ConversionUtils.UriComponents(scheme, host, port, path, query, fragment)       
        actual  must_== expected
      }
    }
}

class StringToDoublelikeTest extends Specification with DataTables with ValidationMatchers { def is =

  "This is a specification to test the stringToDoublelike function"                                       ^
                                                                                                         p^
  "stringToDoublelike should fail if the supplied String is not parseable as a number"                    ! e1^
  "stringToDoublelike should convert numeric Strings to 'Double-like' Strings loadable by Redshift"       ! e2^
  "stringToDoublelike will alas *not* fail numbers having more significant digits than Redshift supports" ! e3^
                                                                                                          end

  val FieldName = "val"
  def err: (String) => String = input => "Field [%s]: cannot convert [%s] to Double-like String".format(FieldName, input)

  def e1 =
    "SPEC NAME"             || "INPUT STR"       | "EXPECTED"            |
    "Empty string"          !! ""                ! err("")               |
    "Number with commas"    !! "19,999.99"       ! err("19,999.99")      |
    "Hexadecimal number"    !! "0x54"            ! err("0x54")           |
    "Bad sci. notation"     !! "-7.51E^9"        ! err("-7.51E^9")       |
    "German number"         !! "1.000,3932"      ! err("1.000,3932")     |
    "NaN"                   !! "NaN"             ! err("NaN")            |
    "English string"        !! "hi & bye"        ! err("hi & bye")       |
    "Vietnamese name"       !! "Trịnh Công Sơn"  ! err("Trịnh Công Sơn") |> {
      (_, str, expected) =>
        ConversionUtils.stringToDoublelike(FieldName, str) must beFailing(expected)
    }

  def e2 =
    "SPEC NAME"             || "INPUT STR"       | "EXPECTED"            |
    "Integer #1"            !! "23"              ! "23"                  |
    "Integer #2"            !! "23."             ! "23"                  |
    "Negative integer"      !! "-2012103"        ! "-2012103"            |
    "Arabic number"         !! "٤٥٦٧.٦٧"         ! "4567.67"             |
    "Floating point #1"     !! "1999.99"         ! "1999.99"             |
    "Floating point #2"     !! "1999.00"         ! "1999.00"             |
    "Floating point #3"     !! "78694353.00001"  ! "78694353.00001"      |
    "Floating point #4"     !! "-78694353.00001" ! "-78694353.00001"     |
    "Sci. notation #1"      !! "4.321768E3"      ! "4321.768"            |
    "Sci. notation #2"      !! "6.72E9"          ! "6720000000"          |
    "Sci. notation #3"      !! "7.51E-9"         ! "0.00000000751"       |> {
      (_, str, expected) =>
        ConversionUtils.stringToDoublelike(FieldName, str) must beSuccessful(expected)
    }

  val BigNumber = "78694235323.00000001" // Redshift only supports 15 significant digits for a Double
  def e3 = ConversionUtils.stringToDoublelike(FieldName, BigNumber) must beSuccessful(BigNumber)

}