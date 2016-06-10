/*
 * Copyright (c) 2016 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.enrich.spark
package good


// Specs2
import org.specs2.mutable.Specification

/**
 * Holds the input and expected data
 * for the test.
 */
object CljTomcatStatusGatorEventSpec {
  import EnrichJobSpec._
  val lines = Lines(
    "2014-10-09  16:28:31    -   13  255.255.255.255   POST    255.255.255.255   /com.statusgator/v1   404 -  -    aid=email&cv=clj-0.6.0-tom-0.0.4&nuid=-   -   -   -   application%2Fx-www-form-urlencoded   bGFzdF9zdGF0dXM9d2FybiZzdGF0dXNfcGFnZV91cmw9aHR0cHMlM0ElMkYlMkZ3d3cuY2xvdWRmbGFyZXN0YXR1cy5jb20lMkYmc2VydmljZV9uYW1lPUNsb3VkRmxhcmUmZmF2aWNvbl91cmw9aHR0cHMlM0ElMkYlMkZkd3hqZDljZDZyd25vLmNsb3VkZnJvbnQubmV0JTJGZmF2aWNvbnMlMkZjbG91ZGZsYXJlLmljbyZvY2N1cnJlZF9hdD0yMDE2LTA1LTE5VDA5JTNBMjYlM0EzMSUyQjAwJTNBMDAmaG9tZV9wYWdlX3VybD1odHRwJTNBJTJGJTJGd3d3LmNsb3VkZmxhcmUuY29tJmN1cnJlbnRfc3RhdHVzPXVw"
    )

  val expected = List(
    "email",
    "srv",
    etlTimestamp,
    "2014-10-09 16:28:31.000",
    null,
    "unstruct",
    null, // We can't predict the event_id
    null,
    null, // No tracker namespace
    "com.statusgator-v1",
    "clj-0.6.0-tom-0.0.4",
    etlVersion,
    null, // No user_id set
    "255.255.x.x",
    null,
    null,
    null,
    "-", // TODO: fix this, https://github.com/snowplow/snowplow/issues/1133
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
    null,
    null,
    null,
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
    """{"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0","data":{"schema":"iglu:com.statusgator/status_change/jsonschema/1-0-0","data":{"last_status":"warn","status_page_url":"https://www.cloudflarestatus.com/","service_name":"CloudFlare","favicon_url":"https://dwxjd9cd6rwno.cloudfront.net/favicons/cloudflare.ico","occurred_at":"2016-05-19T09:26:31+00:00","home_page_url":"http://www.cloudflare.com","current_status":"up"}}}""",
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
    null
    )
}

class CljTomcatStatusGatorEventSpec extends Specification with EnrichJobSpec {
  import EnrichJobSpec._
  override def appName = "clj-tomcat-statusgator-event"
  sequential
  "A job which processes a Clojure-Tomcat file containing a StatusGator POST raw event representing 1 valid completed call" should {

    runEnrichJob(CljTomcatStatusGatorEventSpec.lines, "clj-tomcat", "2", true, List("geo"))

    "correctly output 1 completed call" in {
      val Some(goods) = readPartFile(dirs.output)
      goods.size must_== 1
      val actual = goods.head.split("\t").map(s => if (s.isEmpty()) null else s)
      for (idx <- CljTomcatStatusGatorEventSpec.expected.indices) {
        actual(idx) must BeFieldEqualTo(CljTomcatStatusGatorEventSpec.expected(idx), idx)
      }
    }

    "not write any bad rows" in {
      dirs.badRows must beEmptyDir
    }
  }
}
