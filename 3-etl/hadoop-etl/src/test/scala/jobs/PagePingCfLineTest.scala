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
package jobs

// Scala
import scala.collection.mutable.Buffer

// Specs2
import org.specs2.mutable.Specification

// Scalding
import com.twitter.scalding._

// Cascading
import cascading.tuple.TupleEntry

// This project
import JobTestHelpers._

/**
 * Integration test for the EtlJob:
 *
 * Check that all tuples in a page ping
 * (CloudFront format) are successfully
 * extracted.
 */
class PagePingCfLineTest extends Specification with TupleConversions {

  val input = Lines(
    "2012-05-27  11:35:53  DFW3  3343  99.116.172.58 GET d3gs014xn8p70.cloudfront.net  /ice.png  200 http://www.psychicbazaar.com/oracles/119-psycards-book-and-deck-starter-pack.html?view=print#detail Mozilla/5.0%20(Windows%20NT%206.1;%20WOW64;%20rv:12.0)%20Gecko/20100101%20Firefox/12.0  &e=pp&page=Tarot%20cards%20-%20Psychic%20Bazaar&pp_mix=21&pp_max=214&pp_miy=251&pp_may=517&dtm=1364232736230&tid=209801&vp=923x905&ds=1120x1420&vid=43&duid=9795bd0203804cd1&p=web&tv=js-0.11.1&fp=2876815413&aid=pbzsite&lang=en-GB&cs=UTF-8&tz=Europe%2FLondon&refr=http%3A%2F%2Fwww.psychicbazaar.com%2F&f_pdf=1&f_qt=0&f_realp=0&f_wma=0&f_dir=0&f_fla=1&f_java=1&f_gears=0&f_ag=1&res=2560x1440&cd=32&cookie=1&url=http%3A%2F%2Fwww.psychicbazaar.com%2F2-tarot-cards"
    )

  val expected = List(
    "pbzsite",
    "web",
    "2012-05-27 11:35:53.000",
    "2013-03-25 17:32:16.230",
    "page_ping",
    "com.snowplowanalytics",
    null, // We can't predict the event_id
    "209801",
    "js-0.11.1",
    "cloudfront",
    "hadoop-0.1.0",
    null, // No user_id set
    "99.116.172.58",
    "2876815413",
    "9795bd0203804cd1",
    "43",
    null, // No network_userid set
    // Raw page URL is discarded 
    "Tarot cards - Psychic Bazaar",
    "http://www.psychicbazaar.com/",
    "http",
    "www.psychicbazaar.com",
    "80",
    "/oracles/119-psycards-book-and-deck-starter-pack.html",
    "view=print",
    "detail",
    null, // No marketing campaign info
    null, //
    null, //
    null, //
    null, //
    null, // Event fields empty
    null, //
    null, //
    null, //
    null, //
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
    "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:12.0) Gecko/20100101 Firefox/12.0",
    "Firefox 12",
    "Firefox",
    "12.0",
    "Browser",
    "GECKO",
    "21",  // Page ping fields are set
    "214", //
    "251", //
    "517", //
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
    "923",
    "905",
    "Windows",
    "Windows",
    "Microsoft Corporation",
    "Europe/London",
    "Computer",
    "0",
    "2560",
    "1440",
    "UTF-8",
    "1120",
    "1420"
    )

  "A job which processes a CloudFront file containing 1 valid page ping" should {
    EtlJobTest.
      source(MultipleTextLineFiles("inputFolder"), input).
      sink[TupleEntry](Tsv("outputFolder")){ buf : Buffer[TupleEntry] =>
        "correctly output a page ping" in {
          buf.size must_== 1
          val actual = buf.head
          for (idx <- expected.indices) {
            if (idx != 6) { // We can't predict the event_id
              actual.getString(idx) must_== expected(idx)
            }
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