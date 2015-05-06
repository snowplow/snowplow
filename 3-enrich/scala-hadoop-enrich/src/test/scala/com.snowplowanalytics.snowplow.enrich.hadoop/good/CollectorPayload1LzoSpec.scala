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
import scala.collection.JavaConversions._

// Specs2
import org.specs2.mutable.Specification

// Scalding
import com.twitter.scalding._

// Cascading
import cascading.tuple.TupleEntry

// Apache Thrift
import org.apache.thrift.TSerializer

import JobSpecHelpers._

import com.snowplowanalytics.snowplow.CollectorPayload.thrift.model1.CollectorPayload

/**
 * Holds the input and expected data
 * for the test.
 */
object CollectorPayload1LzoSpec {

  val payloadData = "e=pp&page=Loading%20JSON%20data%20into%20Redshift%20-%20the%20challenges%20of%20quering%20JSON%20data%2C%20and%20how%20Snowplow%20can%20be%20used%20to%20meet%20those%20challenges&pp_mix=0&pp_max=1&pp_miy=64&pp_may=935&cx=eyJwYWdlIjp7InVybCI6ImJsb2cifX0&dtm=1398762054889&tid=612876&vp=1279x610&ds=1279x5614&vid=2&duid=44082d3af0e30126&p=web&tv=js-2.0.0&fp=2071613637&aid=snowplowweb&lang=fr&cs=UTF-8&tz=Europe%2FBerlin&tna=cloudfront&evn=com.snowplowanalytics&refr=http%3A%2F%2Fsnowplowanalytics.com%2Fservices%2Fpipelines.html&f_pdf=1&f_qt=1&f_realp=0&f_wma=0&f_dir=0&f_fla=1&f_java=1&f_gears=0&f_ag=0&res=1280x800&cd=24&cookie=1&url=http%3A%2F%2Fsnowplowanalytics.com%2Fblog%2F2013%2F11%2F20%2Floading-json-data-into-redshift%2F%23weaknesses"

  val collectorPayload = new CollectorPayload(
    "iglu:com.snowplowanalytics.snowplow/CollectorPayload/thrift/1-0-0",
    "255.255.255.255",
    1381175274000L,
    "UTF-8",
    "collector"
  )
  collectorPayload.setHeaders(List("X-Forwarded-For: 123.123.123.123, 345.345.345.345"))
  collectorPayload.setPath("/i")
  collectorPayload.setHostname("localhost")
  collectorPayload.setQuerystring(payloadData)
  collectorPayload.setUserAgent("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_6_8) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/31.0.1650.8 Safari/537.36")
  collectorPayload.setNetworkUserId("8712a379-4bcb-46ee-815d-85f26540577f")

  val serializer = new TSerializer

  val binaryThrift = serializer.serialize(collectorPayload)

  val lines = List(
    (binaryThrift, 1L)
  )

  val expected = List(
    "snowplowweb",
    "web",
    EtlTimestamp,
    "2013-10-07 19:47:54.000",
    "2014-04-29 09:00:54.889",
    "page_ping",
    null, // We can't predict the event_id
    "612876",
    "cloudfront", // Tracker namespace
    "js-2.0.0",
    "collector",
    EtlVersion,
    null, // No user_id set
    "123.123.123.x",
    "2071613637",
    "44082d3af0e30126",
    "2",
    "8712a379-4bcb-46ee-815d-85f26540577f",
    null, // No geo-location for this IP address
    null,
    null,
    null,
    null,
    null,
    null,
    null, // No additional MaxMind databases used
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
    null, // Unstructured event field empty
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
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_6_8) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/31.0.1650.8 Safari/537.36", // previously "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/34.0.1847.131 Safari/537.36",
    "Chrome 31", // previously "Chrome"
    "Chrome",
    "31.0.1650.8",// previously "34.0.1847.131"
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
    "Mac OS X",
    "Mac OS X",
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
 * Test that a raw Thrift event is processed correctly.
 * See https://github.com/snowplow/snowplow/issues/538
 * Based on Apr2014CfLineSpec.
 */
class CollectorPayload1LzoSpec extends Specification {

  "A job which processes a RawThrift file containing 1 valid page view" should {
    EtlJobSpec("thrift", "1", true, List("geo")).
      source(FixedPathLzoRaw("inputFolder"), CollectorPayload1LzoSpec.lines).
      sink[TupleEntry](Tsv("outputFolder")){ buf : Buffer[TupleEntry] =>
        "correctly output 1 page view" in {
          buf.size must_== 1
          val actual = buf.head
          for (idx <- CollectorPayload1LzoSpec.expected.indices) {
            actual.getString(idx) must beFieldEqualTo(CollectorPayload1LzoSpec.expected(idx), withIndex = idx)
          }
        }
      }.
      sink[TupleEntry](Tsv("exceptionsFolder")){ trap =>
        "not trap any exceptions" in {
          trap must beEmpty
        }
      }.
      sink[String](Tsv("badFolder")){ error =>
        "not write any bad rows" in {
          error must beEmpty
        }
      }.
      run.
      finish
  }
}
