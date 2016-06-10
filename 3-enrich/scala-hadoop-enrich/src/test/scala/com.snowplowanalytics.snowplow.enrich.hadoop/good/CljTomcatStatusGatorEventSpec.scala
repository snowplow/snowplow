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
object CljTomcatStatusGatorEventSpec {

  val lines = Lines(
    "2014-10-09  16:28:31    -   13  255.255.255.255   POST    255.255.255.255   /com.statusgator/v1   404 -  -    aid=email&cv=clj-0.6.0-tom-0.0.4&nuid=-   -   -   -   application%2Fx-www-form-urlencoded   bWFuZHJpbGxfZXZlbnRzPSU1QiUwQSUyMCUyMCUyMCUyMCU3QiUwQSUyMCUyMCUyMCUyMCUyMCUyMCUyMCUyMCUyMmV2ZW50JTIyJTNBJTIwJTIyc2VuZCUyMiUyQyUwQSUyMCUyMCUyMCUyMCUyMCUyMCUyMCUyMCUyMm1zZyUyMiUzQSUyMCU3QiUwQSUyMCUyMCUyMCUyMCUyMCUyMCUyMCUyMCUyMCUyMCUyMCUyMCUyMnRzJTIyJTNBJTIwMTM2NTEwOTk5OSUyQyUwQSUyMCUyMCUyMCUyMCUyMCUyMCUyMCUyMCUyMCUyMCUyMCUyMCUyMnN1YmplY3QlMjIlM0ElMjAlMjJUaGlzJTIwYW4lMjBleGFtcGxlJTIwd2ViaG9vayUyMG1lc3NhZ2UlMjIlMkMlMEElMjAlMjAlMjAlMjAlMjAlMjAlMjAlMjAlMjAlMjAlMjAlMjAlMjJlbWFpbCUyMiUzQSUyMCUyMmV4YW1wbGUud2ViaG9vayU0MG1hbmRyaWxsYXBwLmNvbSUyMiUyQyUwQSUyMCUyMCUyMCUyMCUyMCUyMCUyMCUyMCUyMCUyMCUyMCUyMCUyMnNlbmRlciUyMiUzQSUyMCUyMmV4YW1wbGUuc2VuZGVyJTQwbWFuZHJpbGxhcHAuY29tJTIyJTJDJTBBJTIwJTIwJTIwJTIwJTIwJTIwJTIwJTIwJTIwJTIwJTIwJTIwJTIydGFncyUyMiUzQSUyMCU1QiUwQSUyMCUyMCUyMCUyMCUyMCUyMCUyMCUyMCUyMCUyMCUyMCUyMCUyMCUyMCUyMCUyMCUyMndlYmhvb2stZXhhbXBsZSUyMiUwQSUyMCUyMCUyMCUyMCUyMCUyMCUyMCUyMCUyMCUyMCUyMCUyMCU1RCUyQyUwQSUyMCUyMCUyMCUyMCUyMCUyMCUyMCUyMCUyMCUyMCUyMCUyMCUyMm9wZW5zJTIyJTNBJTIwJTVCJTVEJTJDJTBBJTIwJTIwJTIwJTIwJTIwJTIwJTIwJTIwJTIwJTIwJTIwJTIwJTIyY2xpY2tzJTIyJTNBJTIwJTVCJTVEJTJDJTBBJTIwJTIwJTIwJTIwJTIwJTIwJTIwJTIwJTIwJTIwJTIwJTIwJTIyc3RhdGUlMjIlM0ElMjAlMjJzZW50JTIyJTJDJTBBJTIwJTIwJTIwJTIwJTIwJTIwJTIwJTIwJTIwJTIwJTIwJTIwJTIybWV0YWRhdGElMjIlM0ElMjAlN0IlMEElMjAlMjAlMjAlMjAlMjAlMjAlMjAlMjAlMjAlMjAlMjAlMjAlMjAlMjAlMjAlMjAlMjJ1c2VyX2lkJTIyJTNBJTIwMTExJTBBJTIwJTIwJTIwJTIwJTIwJTIwJTIwJTIwJTIwJTIwJTIwJTIwJTdEJTJDJTBBJTIwJTIwJTIwJTIwJTIwJTIwJTIwJTIwJTIwJTIwJTIwJTIwJTIyX2lkJTIyJTNBJTIwJTIyZXhhbXBsZWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWElMjIlMkMlMEElMjAlMjAlMjAlMjAlMjAlMjAlMjAlMjAlMjAlMjAlMjAlMjAlMjJfdmVyc2lvbiUyMiUzQSUyMCUyMmV4YW1wbGVhYWFhYWFhYWFhYWFhYWElMjIlMEElMjAlMjAlMjAlMjAlMjAlMjAlMjAlMjAlN0QlMkMlMEElMjAlMjAlMjAlMjAlMjAlMjAlMjAlMjAlMjJfaWQlMjIlM0ElMjAlMjJleGFtcGxlYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYSUyMiUyQyUwQSUyMCUyMCUyMCUyMCUyMCUyMCUyMCUyMCUyMnRzJTIyJTNBJTIwMTQxNTY5MjAzNSUwQSUyMCUyMCUyMCUyMCU3RCUwQSU1RA=="
    )

  val expected = List(
    "email",
    "srv",
    EtlTimestamp,
    "2014-10-09 16:28:31.000",
    null,
    "unstruct",
    null, // We can't predict the event_id
    null,
    null, // No tracker namespace
    "com.statusgator-v1",
    "clj-0.6.0-tom-0.0.4",
    EtlVersion,
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

/**
 * Integration test for the EtlJob:
 *
 * For details:
 * https://forums.aws.amazon.com/thread.jspa?threadID=134017&tstart=0#
 */
class CljTomcatStatusGatorEventSpec extends Specification {

  "A job which processes a Clojure-Tomcat file containing a StatusGator POST raw event representing 1 valid completed call" should {
    EtlJobSpec("clj-tomcat", "2", true, List("geo")).
      source(MultipleTextLineFiles("inputFolder"), CljTomcatStatusGatorEventSpec.lines).
      sink[TupleEntry](Tsv("outputFolder")){ buf : Buffer[TupleEntry] =>
        "correctly output 1 completed call" in {
          buf.size must_== 1
          val actual = buf.head
          for (idx <- CljTomcatStatusGatorEventSpec.expected.indices) {
            actual.getString(idx) must beFieldEqualTo(CljTomcatStatusGatorEventSpec.expected(idx), withIndex = idx)
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
