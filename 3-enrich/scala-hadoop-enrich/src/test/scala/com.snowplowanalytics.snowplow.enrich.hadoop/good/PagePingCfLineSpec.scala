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
object PagePingCfLineSpec {

  val lines = Lines(
    "2013-03-25 02:04:00    GRU1    1047    255.255.255.255    GET d10wr4jwvp55f9.cloudfront.net   /i  200 http://www.psychicbazaar.com/2-tarot-cards/genre/all/type/all?utm_source=google&utm_medium=cpc&utm_term=buy%2Btarot&utm_campaign=spring_sale  Mozilla/5.0%20(Windows%20NT%206.1;%20WOW64)%20AppleWebKit/537.22%20(KHTML,%20like%20Gecko)%20Chrome/25.0.1364.172%20Safari/537.22   &e=pp&page=Tarot%2520cards%2520-%2520Psychic%2520Bazaar&tna=TMP%20TEST&pp_mix=21&pp_max=214&pp_miy=251&pp_may=517&dtm=1364177017342&tid=128574&vp=1366x630&ds=1349x3787&vid=1&duid=132e226e3359a9cd&p=web&tv=js-0.11.1&fp=1640945579&aid=pbzsite&lang=pt-BR&cs=UTF-8&tz=America%252FSao_Paulo&refr=http%253A%252F%252Fwww.psychicbazaar.com%252F2-tarot-cards%252Fgenre%252Fall%252Ftype%252Fall%253Fn%253D48&f_pdf=1&f_qt=0&evn=com.snowplowanalytics&f_realp=1&f_wma=0&f_dir=0&f_fla=1&f_java=1&f_gears=0&f_ag=1&res=1366x768&cd=32&cookie=1&url=http%3A%2F%2Fwww.psychicbazaar.com%2F2-tarot-cards%2Fgenre%2Fall%2Ftype%2Fall%3Futm_source%3Dgoogle%26utm_medium%3Dcpc%26utm_term%3Dbuy%252Btarot%26utm_campaign%3Dspring_sale -   Hit dfFVXBxYoXbfL3TBTlr6Q-_TFqzLujgZBfuAa80qB9ND22Cn5lqJdg=="
    )

  val expected = List(
    "pbzsite",
    "web",
    "2013-03-25 02:04:00.000",
    "2013-03-25 02:03:37.342",
    "page_ping",
    "com.snowplowanalytics",
    null, // We can't predict the event_id
    "128574",
    "TMP TEST", // Tracker namespace
    "js-0.11.1",
    "cloudfront",
    EtlVersion,
    null, // No user_id set
    "255.255.255.255",
    "1640945579",
    "132e226e3359a9cd",
    "1",
    null, // No network_userid set
    null, // No geo-location for this IP address
    null,
    null,
    null,
    null,
    null,
    "http://www.psychicbazaar.com/2-tarot-cards/genre/all/type/all?utm_source=google&utm_medium=cpc&utm_term=buy+tarot&utm_campaign=spring_sale",
    "Tarot cards - Psychic Bazaar",
    "http://www.psychicbazaar.com/2-tarot-cards/genre/all/type/all?n=48",
    "http",
    "www.psychicbazaar.com",
    "80",
    "/2-tarot-cards/genre/all/type/all",
    "utm_source=google&utm_medium=cpc&utm_term=buy+tarot&utm_campaign=spring_sale",
    null,
    "http",
    "www.psychicbazaar.com",
    "80",
    "/2-tarot-cards/genre/all/type/all",
    "n=48",
    null,
    "internal", // Internal referer
    null,
    null,
    "cpc",
    "google",
    "buy tarot",
    null,
    "spring_sale",
    null, // No custom contexts
    null, // Structured event fields empty
    null, //
    null, //
    null, //
    null, //
    null, // Unstructured event fields empty
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
    "21",  // Page ping fields are set
    "214", //
    "251", //
    "517", //
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.22 (KHTML, like Gecko) Chrome/25.0.1364.172 Safari/537.22",
    "Chrome 25",
    "Chrome",
    "25.0.1364.172", // Yech. We need to upgrade our UA library
    "Browser",
    "WEBKIT",
    "pt-BR",
    "1",
    "1",
    "1",
    "0",
    "0",
    "1",
    "0",
    "0",
    "1",
    "1",
    "32",
    "1366",
    "630",
    "Windows",
    "Windows",
    "Microsoft Corporation",
    "America/Sao_Paulo",
    "Computer",
    "0",
    "1366",
    "768",
    "UTF-8",
    "1349",
    "3787"
    )
}

/**
 * Integration test for the EtlJob:
 *
 * Check that all tuples in a page ping
 * (CloudFront format) are successfully
 * extracted.
 */
class PagePingCfLineSpec extends Specification with TupleConversions {

  "A job which processes a CloudFront file containing 1 valid page ping" should {
    EtlJobSpec("cloudfront", "0").
      source(MultipleTextLineFiles("inputFolder"), PagePingCfLineSpec.lines).
      sink[TupleEntry](Tsv("outputFolder")){ buf : Buffer[TupleEntry] =>
        "correctly output 1 page ping" in {
          buf.size must_== 1
          val actual = buf.head
          for (idx <- PagePingCfLineSpec.expected.indices) {
            actual.getString(idx) must beFieldEqualTo(PagePingCfLineSpec.expected(idx), withIndex = idx)
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