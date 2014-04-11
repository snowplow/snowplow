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
package com.snowplowanalytics.snowplow.enrich
package hadoop
package good

// Scala
import scala.collection.mutable.Buffer

// Specs2
import org.specs2.mutable.Specification

// Scalding
import com.twitter.scalding._

// Cascading
import cascading.tuple.TupleEntry

// This project
import JobSpecHelpers._

/**
 * Holds the input and expected data
 * for the test.
 */
object UnstructEventCfLineSpec {

  val lines = Lines(
    "2012-05-27  11:35:53  DFW3  3343  99.116.172.58 GET d3gs014xn8p70.cloudfront.net  /ice.png  200 http://www.psychicbazaar.com/oracles/119-psycards-book-and-deck-starter-pack.html?view=print#detail Mozilla/5.0%20(Windows%20NT%206.1;%20WOW64;%20rv:12.0)%20Gecko/20100101%20Firefox/12.0  &e=ue&ue_na=viewed_product&ue_px=eyJhZ2UiOjIzLCJuYW1lIjoiSm9obiJ9&dtm=1364230969450&evn=com.acme&tid=598951&vp=2560x934&ds=2543x1420&vid=43&duid=9795bd0203804cd1&p=web&tv=js-0.11.1&fp=2876815413&aid=pbzsite&lang=en-GB&cs=UTF-8&tz=Europe%2FLondon&refr=http%3A%2F%2Fwww.psychicbazaar.com%2F&f_pdf=1&f_qt=0&f_realp=0&f_wma=0&f_dir=0&f_fla=1&f_java=1&f_gears=0&f_ag=1&res=2560x1440&cd=32&cookie=1&url=http%3A%2F%2Fwww.psychicbazaar.com%2F2-tarot-cards"
    )

  val expected = List(
    "pbzsite",
    "web",
    "2012-05-27 11:35:53.000",
    "2013-03-25 17:02:49.450",
    "unstruct",
    "com.acme",
    null, // We can't predict the event_id
    "598951",
    null, // No tracker namespace
    "js-0.11.1",
    "cloudfront",
    EtlVersion,
    null, // No user_id set
    "99.116.172.58",
    "2876815413",
    "9795bd0203804cd1",
    "43",
    null, // No network_userid set
    "US", // No geo-location for this IP address
    "KS",
    "Haysville",
    "67060",
    "37.543304",
    "-97.3657",
    "http://www.psychicbazaar.com/2-tarot-cards",
    null, // No page title for events
    "http://www.psychicbazaar.com/",
    "http",
    "www.psychicbazaar.com",
    "80",
    "/2-tarot-cards",
    null,
    null,
    "http",
    "www.psychicbazaar.com",
    "80",
    "/",
    null,
    null,
    "internal", // Internal referer
    null,
    null,
    null, // No marketing campaign info
    null, //
    null, //
    null, //
    null, //
    null, // No custom contexts
    null, // Structured event fields empty
    null, //
    null, //
    null, //
    null, //
    "viewed_product",               // Unstructured event fields set
    """{"age":23,"name":"John"}""", //
    null, // Transaction fields empty
    null, //
    null, //
    null, //
    null, //
    null, //
    null, //
    null, //
    null, // Transaction item fields empty
    null, //
    null, //
    null, //
    null, //
    null, //
    null, // Page ping fields are empty
    null, //
    null, //
    null, //
    "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:12.0) Gecko/20100101 Firefox/12.0",
    "Firefox 12",
    "Firefox",
    "12.0",
    "Browser",
    "GECKO",
    "en-GB",
    "1",
    "1",
    "1",
    "0",
    "0",
    "0",
    "0",
    "0",
    "1",
    "1",
    "32",
    "2560",
    "934",
    "Windows",
    "Windows",
    "Microsoft Corporation",
    "Europe/London",
    "Computer",
    "0",
    "2560",
    "1440",
    "UTF-8",
    "2543",
    "1420"
    )
}

/**
 * Integration test for the EtlJob:
 *
 * Check that all tuples in a custom unstructured event
 * (CloudFront format) are successfully extracted.
 */
class UnstructEventCfLineSpec extends Specification with TupleConversions {

  "A job which processes a CloudFront file containing 1 valid custom unstructured event" should {
    EtlJobSpec("cloudfront", "0").
      source(MultipleTextLineFiles("inputFolder"), UnstructEventCfLineSpec.lines).
      sink[TupleEntry](Tsv("outputFolder")){ buf : Buffer[TupleEntry] =>
        "correctly output 1 custom unstructured event" in {
          buf.size must_== 1
          val actual = buf.head
          for (idx <- UnstructEventCfLineSpec.expected.indices) {
            actual.getString(idx) must beFieldEqualTo(UnstructEventCfLineSpec.expected(idx), withIndex = idx)
          }
        }
      }.
      sink[TupleEntry](Tsv("exceptionsFolder")){ trap =>
        "not trap any exceptions" in {
          trap must beEmpty
        }
      }.
      sink[String](JsonLine("badFolder")){ error =>
        "not write any bad rows" in {
          error must beEmpty
        }
      }.
      run.
      finish
  }
}