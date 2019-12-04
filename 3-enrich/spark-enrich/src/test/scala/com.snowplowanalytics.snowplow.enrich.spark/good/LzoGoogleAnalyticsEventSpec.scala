/*
 * Copyright (c) 2018-2019 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at
 * http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and
 * limitations there under.
 */
package com.snowplowanalytics.snowplow
package enrich.spark
package good

import scala.collection.JavaConverters._

import org.specs2.mutable.Specification

import CollectorPayload.thrift.model1.CollectorPayload

object LzoGoogleAnalyticsEventSpec {
  import EnrichJobSpec._

  val payloadData = "t=pageview&dh=host&dp=path"
  val collectorPayload = new CollectorPayload(
    "iglu:com.snowplowanalytics.snowplow/CollectorPayload/thrift/1-0-0",
    "255.255.255.255",
    1381175274000L,
    "UTF-8",
    "collector"
  )
  collectorPayload.setHeaders(List("X-Forwarded-For: 123.123.123.123, 345.345.345.345").asJava)
  collectorPayload.setPath("/com.google.analytics/v1")
  collectorPayload.setHostname("localhost")
  collectorPayload.setBody(payloadData)
  collectorPayload.setUserAgent(
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_6_8) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/31.0.1650.8 Safari/537.36")
  collectorPayload.setNetworkUserId("8712a379-4bcb-46ee-815d-85f26540577f")

  val expected = List(
    null,
    "srv",
    etlTimestamp,
    "2013-10-07 19:47:54.000",
    null,
    "unstruct",
    null, // We can't predict the event_id
    null,
    null, // Tracker namespace
    "com.google.analytics.measurement-protocol-v1",
    "collector",
    etlVersion,
    null, // No user_id set
    "a125dd343866ca654dc79abf258b3971215c59ed",
    null,
    null,
    null,
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
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null, // Internal referer
    null,
    null,
    null, // Marketing campaign fields empty
    null, //
    null, //
    null, //
    null, //
    """{"schema":"iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-1","data":[{"schema":"iglu:com.google.analytics.measurement-protocol/hit/jsonschema/1-0-0","data":{"type":"pageview"}}]}""",
    null, // Structured event fields empty
    null, //
    null, //
    null, //
    null, //
    """{"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0","data":{"schema":"iglu:com.google.analytics.measurement-protocol/page_view/jsonschema/1-0-0","data":{"documentHostName":"host","documentPath":"path"}}}""", // Unstructured event field
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
    null, // Page ping fields
    null, //
    null, //
    null, //
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_6_8) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/31.0.1650.8 Safari/537.36", // previously "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/34.0.1847.131 Safari/537.36",
    "Chrome 31", // previously "Chrome"
    "Chrome",
    "31.0.1650.8", // previously "34.0.1847.131"
    "Browser",
    "WEBKIT",
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    "Mac OS X",
    "Mac OS X",
    "Apple Inc.",
    null,
    "Computer",
    "0",
    null,
    null,
    null,
    null,
    null
  )
}

/**
 * Test that a raw Thrift event is processed correctly.
 * See https://github.com/snowplow/snowplow/issues/538
 * Based on Apr2014CfLineSpec.
 */
class LzoGoogleAnalyticsEventSpec extends Specification with EnrichJobSpec {
  import EnrichJobSpec._
  override def appName = "lzo-google-analytics-event"
  sequential
  "A job which processes a RawThrift file containing 1 valid google analytics page view" should {

    if (!isLzoSupported) "native-lzo not supported" in skipped
    else {
      val f = writeLzo("google-analytics", LzoGoogleAnalyticsEventSpec.collectorPayload)
      runEnrichJob(
        f.toString(),
        "thrift",
        "1",
        true,
        List("geo"),
        false,
        false,
        false,
        false,
        false,
        false)

      "correctly output 1 google analytics page view" in {
        val Some(goods) = readPartFile(dirs.output)
        goods.size must_== 1
        val actual = goods.head.split("\t").map(s => if (s.isEmpty()) null else s)
        for (idx <- LzoGoogleAnalyticsEventSpec.expected.indices) {
          actual(idx) must BeFieldEqualTo(LzoGoogleAnalyticsEventSpec.expected(idx), idx)
        }
      }

      "not write any bad rows" in {
        dirs.badRows must beEmptyDir
      }
    }
  }
}
