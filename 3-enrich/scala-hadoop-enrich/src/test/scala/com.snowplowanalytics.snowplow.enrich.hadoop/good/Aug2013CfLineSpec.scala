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
object Aug2013CfLineSpec {

  // August 2013: Amazon broke the CloudFront access log file format. They stopped double-encoding the querystring
  val lines = Lines(
    "2013-08-29	00:18:48	LAX3	830	255.255.255.255	GET	d3v6ndkyapxc2w.cloudfront.net	/i	200	http://snowplowanalytics.com/analytics/index.html	Mozilla/5.0%20(Windows%20NT%205.1;%20rv:23.0)%20Gecko/20100101%20Firefox/23.0	e=pv&page=Introduction%20-%20Snowplow%20Analytics%25&dtm=1377735557970&tid=567074&vp=1024x635&ds=1024x635&vid=1&tna=main&duid=7969620089de36eb&p=web&tv=js-0.12.0&fp=308909339&aid=snowplowweb&lang=en-US&cs=UTF-8&tz=America%2FLos_Angeles&refr=http%3A%2F%2Fwww.metacrawler.com%2Fsearch%2Fweb%3Ffcoid%3D417%26fcop%3Dtopnav%26fpid%3D27%26q%3Dsnowplow%2Banalytics%26ql%3D&f_pdf=1&f_qt=1&f_realp=0&f_wma=1&f_dir=0&f_fla=1&f_java=1&f_gears=0&f_ag=0&res=1024x768&cd=24&cookie=1&url=http%3A%2F%2Fsnowplowanalytics.com%2Fanalytics%2Findex.html	-	Hit	wQ1OBZtQlGgfM_tPEJ-lIQLsdra0U-lXgmfJfwja2KAV_SfTdT3lZg=="
    )

  val expected = List(
    "snowplowweb",
    "web",
    "2013-08-29 00:18:48.000",
    "2013-08-29 00:19:17.970",
    "page_view",
    null, // No event vendor set
    null, // We can't predict the event_id
    "567074",
    "main", // Tracker namespace
    "js-0.12.0",
    "cloudfront",
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
    "http://snowplowanalytics.com/analytics/index.html",
    "Introduction - Snowplow Analytics%",
    "http://www.metacrawler.com/search/web?fcoid=417&fcop=topnav&fpid=27&q=snowplow+analytics&ql=",
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
    null, // Page ping fields empty
    null, //
    null, //
    null, //
    "Mozilla/5.0 (Windows NT 5.1; rv:23.0) Gecko/20100101 Firefox/23.0",
    "Firefox 23",
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
 * CloudFront format changed in August 2013
 * are successfully extracted.
 *
 * For details:
 * https://forums.aws.amazon.com/thread.jspa?threadID=134017&tstart=0#
 */
class Aug2013CfLineSpec extends Specification with TupleConversions {

  "A job which processes a CloudFront file containing 1 valid page view" should {
    EtlJobSpec("cloudfront", "0").
      source(MultipleTextLineFiles("inputFolder"), Aug2013CfLineSpec.lines).
      sink[TupleEntry](Tsv("outputFolder")){ buf : Buffer[TupleEntry] =>
        "correctly output 1 page ping" in {
          buf.size must_== 1
          val actual = buf.head
          for (idx <- Aug2013CfLineSpec.expected.indices) {
            actual.getString(idx) must beFieldEqualTo(Aug2013CfLineSpec.expected(idx), withIndex = idx)
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