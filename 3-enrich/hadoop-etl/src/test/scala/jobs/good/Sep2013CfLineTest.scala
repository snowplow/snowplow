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
object Sep2013CfLineTest {

  // September 2013: UA and referer are now double-encoded, querystring is single-encoded
  val lines = Lines(
    "2013-10-07	21:32:22	SFO5	828	66.249.82.34	GET	d10wr4jwvp55f9.cloudfront.net	/i	200	http://www.psychicbazaar.com/tarot-cards/312-dreaming-way-tarot.html?utm_source=GoogleSearch&utm_term=rome%2520choi%2520tarot&utm_content=42017424088&utm_medium=cpc&utm_campaign=uk-tarot-decks--pbz00316	IE%25207%2520-%2520Mozilla/4.0%2520(compatible;%2520MSIE%25207.0;%2520Windows%2520NT%25205.1;%2520.NET%2520CLR%25201.1.4322;%2520.NET%2520CLR%25202.0.50727;%2520.NET%2520CLR%25203.0.04506.30)	e=pv&page=Dreaming%20Way%20Tarot%20-%20Psychic%20Bazaar&dtm=1381181437923&tid=390328&vp=1003x611&ds=1063x1483&vid=1&duid=2e99db5bd6a5150c&p=web&tv=js-0.12.0&fp=408352165&aid=pbzsite&lang=en-us&cs=utf-8&tz=America%2FSanta_Isabel&refr=http%3A%2F%2Fwww.google.com%2Furl%3Fsa%3Dt%26rct%3Dj%26q%3Dwww.psychicbazaar.com%252B312-dreaming-way-tarot%26source%3Dweb%26cd%3D1%26ved%3D0CFwQFjAD%26url%3Dhttp%253A%252F%252Fwww.psychicbazaar.com%252Ftarot-cards%252F312-dreaming-way-tarot.html%253Futm_source%253DGoogleSearch%2526utm_term%253Drome%252520choi%252520tarot%2526utm_content%253D42017424088%2526utm_medium%253Dcpc%2526utm_campaign%253Duk-tarot-decks--pbz00316%26ei%3D2CdTUo3DJqf9oAa_Fg%26usg%3DAFQjCNGGq8p48SyYds9oznKs1F5RQYtx_A&res=1024x768&cd=24&cookie=1&url=http%3A%2F%2Fwww.psychicbazaar.com%2Ftarot-cards%2F312-dreaming-way-tarot.html%3Futm_source%3DGoogleSearch%26utm_term%3Drome%2520choi%2520tarot%26utm_content%3D42017424088%26utm_medium%3Dcpc%26utm_campaign%3Duk-tarot-decks--pbz00316	-	Hit	BbKc9iUDAsrzS4KcQOA9YPN-9rp7-HGyLHqgu3gINLP40W9OtnWY3A=="
    )

  val expected = List(
    "snowplowweb",
    "web",
    "2013-08-29 00:18:48.000",
    "2013-08-29 00:19:17.970",
    "page_view",
    "com.snowplowanalytics",
    null, // We can't predict the event_id
    "567074",
    "js-0.12.0",
    "fixme",
    EtlVersion,
    null, // No user_id set
    "255.255.255.255",
    "308909339",
    "7969620089de36eb",
    "1",
    null, // No network_userid set
    null, // No geo-location for this IP address
    null,
    null,
    null,
    null,
    null,
    // Raw page URL is discarded 
    "Introduction - Snowplow Analytics%",
    // Raw referer URL is discarded
    "http",
    "snowplowanalytics.com",
    "80",
    "/analytics/index.html",
    null,
    null,
    "http",
    "www.metacrawler.com",
    "80",
    "/search/web",
    "fcoid=417&fcop=topnav&fpid=27&q=snowplow+analytics&ql=",
    null,
    "search", // Search referer
    "InfoSpace",
    "snowplow analytics",
    null, // Marketing campaign fields empty
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
    null, // Page ping fields empty
    null, //
    null, //
    null, //
    "Mozilla/5.0 (Windows NT 5.1; rv:23.0) Gecko/20100101 Firefox/23.0",
    "Firefox 2",
    "Firefox",
    "23.0",
    "Browser",
    "GECKO",
    "en-US",
    "1",
    "1",
    "1",
    "0",
    "1",
    "0",
    "1",
    "0",
    "0",
    "1",
    "24",
    "1024",
    "635",
    "Windows",
    "Windows",
    "Microsoft Corporation",
    "America/Los_Angeles",
    "Computer",
    "0",
    "1024",
    "768",
    "UTF-8",
    "1024",
    "635"
    )
}

/**
 * Integration test for the EtlJob:
 *
 * Check that all tuples in a page view in the
 * CloudFront format changed in September 2013
 * are successfully extracted.
 *
 * For details:
 * https://forums.aws.amazon.com/thread.jspa?threadID=134017&tstart=0#
 */
class Sep2013CfLineTest extends Specification with TupleConversions {

  "A job which processes a CloudFront file containing 1 valid page ping" should {
    EtlJobTest("0").
      source(MultipleTextLineFiles("inputFolder"), Sep2013CfLineTest.lines).
      sink[TupleEntry](Tsv("outputFolder")){ buf : Buffer[TupleEntry] =>
        "correctly output 1 page ping" in {
          buf.size must_== 1
          val actual = buf.head
          for (idx <- Sep2013CfLineTest.expected.indices) {
            actual.getString(idx) must beFieldEqualTo(Sep2013CfLineTest.expected(idx), withIndex = idx)
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