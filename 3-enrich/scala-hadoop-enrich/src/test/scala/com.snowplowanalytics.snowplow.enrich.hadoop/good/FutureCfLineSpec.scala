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
object FutureCfLineSpec {

  // Let's imagine a future line format with two new fields on the end
  val lines = Lines(
    "2015-04-29	09:00:54	CDG51	830	255.255.255.255	GET	d3v6ndkyapxc2w.cloudfront.net	/i	200	http://snowplowanalytics.com/blog/2013/11/20/loading-json-data-into-redshift/	Mozilla/5.0%2520(Macintosh;%2520Intel%2520Mac%2520OS%2520X%252010_9_2)%2520AppleWebKit/537.36%2520(KHTML,%2520like%2520Gecko)%2520Chrome/34.0.1847.131%2520Safari/537.36	e=pp&page=Loading%2520JSON%2520data%2520into%2520Redshift%2520-%2520the%2520challenges%2520of%2520quering%2520JSON%2520data%252C%2520and%2520how%2520Snowplow%2520can%2520be%2520used%2520to%2520meet%2520those%2520challenges&pp_mix=0&pp_max=1&pp_miy=64&pp_may=935&cx=eyJwYWdlIjp7InVybCI6ImJsb2cifX0&dtm=1430294454889&tid=612876&vp=1279x610&ds=1279x5614&vid=2&duid=44082d3af0e30126&p=web&tv=js-2.0.0&fp=2071613637&aid=snowplowweb&lang=fr&cs=UTF-8&tz=Europe%252FBerlin&tna=cloudfront&evn=com.snowplowanalytics&refr=http%253A%252F%252Fsnowplowanalytics.com%252Fservices%252Fpipelines.html&f_pdf=1&f_qt=1&f_realp=0&f_wma=0&f_dir=0&f_fla=1&f_java=1&f_gears=0&f_ag=0&res=1280x800&cd=24&cookie=1&url=http%253A%252F%252Fsnowplowanalytics.com%252Fblog%252F2013%252F11%252F20%252Floading-json-data-into-redshift%252F%2523weaknesses	-	Hit	cN-iKWE_3tTwxIKGkSnUOjGpmNjsDUyk4ctemoxU_zIG7Md_fH87sg==	d3v6ndkyapxc2w.cloudfront.net	http	1163	0.001 nowarning 991"
    )

  val expected = List(
    "snowplowweb",
    "web",
    "2015-04-29 09:00:54.000",
    "2015-04-29 08:00:54.889",
    "page_ping",
    "com.snowplowanalytics",
    null, // We can't predict the event_id
    "612876",
    "cloudfront", // Tracker namespace
    "js-2.0.0",
    "cloudfront",
    EtlVersion,
    null, // No user_id set
    "255.255.255.255",
    "2071613637",
    "44082d3af0e30126",
    "2",
    null, // No network_userid set
    null, // No geo-location for this IP address
    null,
    null,
    null,
    null,
    null,
    "http://snowplowanalytics.com/blog/2013/11/20/loading-json-data-into-redshift/#weaknesses",
    "Loading JSON data into Redshift - the challenges of quering JSON data, and how Snowplow can be used to meet those challenges",
    "http://snowplowanalytics.com/services/pipelines.html",
    "http",
    "snowplowanalytics.com",
    "80",
    "/blog/2013/11/20/loading-json-data-into-redshift/",
    null,
    "weaknesses",
    "http",
    "snowplowanalytics.com",
    "80",
    "/services/pipelines.html",
    null,
    null,
    "internal", // Internal referer
    null,
    null,
    null, // Marketing campaign fields empty
    null, //
    null, //
    null, //
    null, //
    """{"page":{"url":"blog"}}""",
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
    "0", // Page ping fields
    "1", //
    "64", //
    "935", //
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/34.0.1847.131 Safari/537.36",
    "Chrome",
    "Chrome",
    "34.0.1847.131",
    "Browser",
    "WEBKIT",
    "fr",
    "1",
    "1",
    "1",
    "0",
    "1",
    "0",
    "0",
    "0",
    "0",
    "1",
    "24",
    "1279",
    "610",
    "Mac OS",
    "Mac OS",
    "Apple Inc.",
    "Europe/Berlin",
    "Computer",
    "0",
    "1280",
    "800",
    "UTF-8",
    "1279",
    "5614"
    )
}

/**
 * Integration test for the EtlJob:
 *
 * Check that all tuples in a page view in the
 * CloudFront format changed in April 2014
 * are successfully extracted.
 *
 * For details:
 * https://forums.aws.amazon.com/thread.jspa?threadID=134017&tstart=0#
 */
class FutureCfLineSpec extends Specification with TupleConversions {

  "A job which processes a CloudFront file containing 1 valid page view" should {
    EtlJobSpec("cloudfront", "0").
      source(MultipleTextLineFiles("inputFolder"), FutureCfLineSpec.lines).
      sink[TupleEntry](Tsv("outputFolder")){ buf : Buffer[TupleEntry] =>
        "correctly output 1 page ping" in {
          buf.size must_== 1
          val actual = buf.head
          for (idx <- FutureCfLineSpec.expected.indices) {
            actual.getString(idx) must beFieldEqualTo(FutureCfLineSpec.expected(idx), withIndex = idx)
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