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
package jobs
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
import JobTestHelpers._

/**
 * Holds the input and expected data
 * for the test.
 */
object TransactionCfLineTest {

  val lines = Lines(
    "2012-05-27  11:35:53  DFW3  3343  99.116.172.58 GET d3gs014xn8p70.cloudfront.net  /ice.png  200 http://www.psychicbazaar.com/oracles/119-psycards-book-and-deck-starter-pack.html?view=print#detail Mozilla/5.0%20(Windows%20NT%206.1;%20WOW64;%20rv:12.0)%20Gecko/20100101%20Firefox/12.0  &e=tr&tr_id=order-123&tr_af=psychicbazaar&tr_tt=8000&tr_tx=200&tr_sh=50&tr_ci=London&tr_st=England&tr_co=UK&tid=028288&duid=a279872d76480afb&vid=1&aid=CFe23a&lang=en-GB&f_pdf=0&f_qt=1&f_realp=0&f_wma=1&f_dir=0&f_fla=1&f_java=1&f_gears=0&f_ag=0&res=1920x1080&cookie=1&url=file%3A%2F%2F%2Fhome%2Falex%2Fasync.html"
    )

  val expected = List(
    "CFe23a",
    null, // Not set (legacy input line)
    "2012-05-27 11:35:53.000",
    null, // Not set (legacy input line)
    "transaction",
    "com.snowplowanalytics",
    null, // We can't predict the event_id
    "028288",
    null, // Not set (legacy input line)
    "cloudfront",
    "hadoop-0.2.0",
    null, // No user_id set
    "99.116.172.58",
    null, // Not set (legacy input line)
    "a279872d76480afb",
    "1",
    null, // No network_userid set
    // Raw page URL is discarded
    null, // No page title for transactions
    // Raw referer URL is discarded
    "http",
    "www.psychicbazaar.com",
    "80",
    "/oracles/119-psycards-book-and-deck-starter-pack.html",
    "view=print",
    "detail",
    null, // No referrer URL components
    null, //
    null, //
    null, //
    null, //
    null, //
    null, // No referrer details
    null, //
    null, //
    null, // No marketing campaign info
    null, //
    null, //
    null, //
    null, //
    null, // Structured event fields empty
    null, //
    null, //
    null, //
    null, //
    null, // Unstructured event fields empty
    null, //
    "order-123",     // Transaction fields are set
    "psychicbazaar", //
    "8000",          //
    "200",           //
    "50",            //
    "London",        //
    "England",       //
    "UK",            //
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
    "0",
    "1",
    "1",
    "0",
    "1",
    "0",
    "1",
    "0",
    "0",
    "1",
    null, // Not set (legacy input lines)
    null, //
    null, //
    "Windows",
    "Windows",
    "Microsoft Corporation",
    null, // Not set (legacy input line)
    "Computer",
    "0",
    "1920",
    "1080",
    null, // Not set (legacy input lines)
    null, //
    null  //
    )
}

/**
 * Integration test for the EtlJob:
 *
 * Check that all tuples in a transaction event
 * (CloudFront format) are successfully extracted.
 */
class TransactionCfLineTest extends Specification with TupleConversions {

  "A job which processes a CloudFront file containing 1 valid transaction" should {
    EtlJobTest.
      source(MultipleTextLineFiles("inputFolder"), TransactionCfLineTest.lines).
      sink[TupleEntry](Tsv("outputFolder")){ buf : Buffer[TupleEntry] =>
        "correctly output 1 transaction" in {
          buf.size must_== 1
          val actual = buf.head
          for (idx <- TransactionCfLineTest.expected.indices) {
            if (idx != 6) { // We can't predict the event_id
              actual.getString(idx) must_== TransactionCfLineTest.expected(idx)
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