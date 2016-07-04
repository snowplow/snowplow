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
object CljTomcatMailgunEventSpec {

  val lines = Lines(
    "2014-10-09  16:28:31    -   13  255.255.255.255   POST    255.255.255.255   /com.mailgun/v1   404 -  -    aid=email&cv=clj-0.6.0-tom-0.0.4&nuid=-   -   -   -   application%2Fx-www-form-urlencoded   bWFuZHJpbGxfZXZlbnRzPSU1QiUwQSUyMCUyMCUyMCUyMCU3QiUwQSUyMCUyMCUyMCUyMCUyMCUyMCUyMCUyMCUyMmV2ZW50JTIyJTNBJTIwJTIyc2VuZCUyMiUyQyUwQSUyMCUyMCUyMCUyMCUyMCUyMCUyMCUyMCUyMm1zZyUyMiUzQSUyMCU3QiUwQSUyMCUyMCUyMCUyMCUyMCUyMCUyMCUyMCUyMCUyMCUyMCUyMCUyMnRzJTIyJTNBJTIwMTM2NTEwOTk5OSUyQyUwQSUyMCUyMCUyMCUyMCUyMCUyMCUyMCUyMCUyMCUyMCUyMCUyMCUyMnN1YmplY3QlMjIlM0ElMjAlMjJUaGlzJTIwYW4lMjBleGFtcGxlJTIwd2ViaG9vayUyMG1lc3NhZ2UlMjIlMkMlMEElMjAlMjAlMjAlMjAlMjAlMjAlMjAlMjAlMjAlMjAlMjAlMjAlMjJlbWFpbCUyMiUzQSUyMCUyMmV4YW1wbGUud2ViaG9vayU0MG1hbmRyaWxsYXBwLmNvbSUyMiUyQyUwQSUyMCUyMCUyMCUyMCUyMCUyMCUyMCUyMCUyMCUyMCUyMCUyMCUyMnNlbmRlciUyMiUzQSUyMCUyMmV4YW1wbGUuc2VuZGVyJTQwbWFuZHJpbGxhcHAuY29tJTIyJTJDJTBBJTIwJTIwJTIwJTIwJTIwJTIwJTIwJTIwJTIwJTIwJTIwJTIwJTIydGFncyUyMiUzQSUyMCU1QiUwQSUyMCUyMCUyMCUyMCUyMCUyMCUyMCUyMCUyMCUyMCUyMCUyMCUyMCUyMCUyMCUyMCUyMndlYmhvb2stZXhhbXBsZSUyMiUwQSUyMCUyMCUyMCUyMCUyMCUyMCUyMCUyMCUyMCUyMCUyMCUyMCU1RCUyQyUwQSUyMCUyMCUyMCUyMCUyMCUyMCUyMCUyMCUyMCUyMCUyMCUyMCUyMm9wZW5zJTIyJTNBJTIwJTVCJTVEJTJDJTBBJTIwJTIwJTIwJTIwJTIwJTIwJTIwJTIwJTIwJTIwJTIwJTIwJTIyY2xpY2tzJTIyJTNBJTIwJTVCJTVEJTJDJTBBJTIwJTIwJTIwJTIwJTIwJTIwJTIwJTIwJTIwJTIwJTIwJTIwJTIyc3RhdGUlMjIlM0ElMjAlMjJzZW50JTIyJTJDJTBBJTIwJTIwJTIwJTIwJTIwJTIwJTIwJTIwJTIwJTIwJTIwJTIwJTIybWV0YWRhdGElMjIlM0ElMjAlN0IlMEElMjAlMjAlMjAlMjAlMjAlMjAlMjAlMjAlMjAlMjAlMjAlMjAlMjAlMjAlMjAlMjAlMjJ1c2VyX2lkJTIyJTNBJTIwMTExJTBBJTIwJTIwJTIwJTIwJTIwJTIwJTIwJTIwJTIwJTIwJTIwJTIwJTdEJTJDJTBBJTIwJTIwJTIwJTIwJTIwJTIwJTIwJTIwJTIwJTIwJTIwJTIwJTIyX2lkJTIyJTNBJTIwJTIyZXhhbXBsZWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWElMjIlMkMlMEElMjAlMjAlMjAlMjAlMjAlMjAlMjAlMjAlMjAlMjAlMjAlMjAlMjJfdmVyc2lvbiUyMiUzQSUyMCUyMmV4YW1wbGVhYWFhYWFhYWFhYWFhYWElMjIlMEElMjAlMjAlMjAlMjAlMjAlMjAlMjAlMjAlN0QlMkMlMEElMjAlMjAlMjAlMjAlMjAlMjAlMjAlMjAlMjJfaWQlMjIlM0ElMjAlMjJleGFtcGxlYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYSUyMiUyQyUwQSUyMCUyMCUyMCUyMCUyMCUyMCUyMCUyMCUyMnRzJTIyJTNBJTIwMTQxNTY5MjAzNSUwQSUyMCUyMCUyMCUyMCU3RCUwQSU1RA=="
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
    "com.mailgun-v1",
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
    """{"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0","data":{"schema":"iglu:com.mailgun/message_delivered/jsonschema/1-0-0","data":{"recipient":"test@snowplowanalytics.com","message-headers":"[[\"Sender\", \"postmaster@sandboxbcd3ccb1a529415db665622619a61616.mailgun.org\"], [\"Date\", \"Mon, 27 Jun 2016 15:19:02 +0000\"], [\"X-Mailgun-Sid\", \"WyIxZjQzMiIsICJyb25ueUBrZGUub3JnIiwgIjliMjYwIl0=\"], [\"Received\", \"by luna.mailgun.net with HTTP; Mon, 27 Jun 2016 15:19:01 +0000\"], [\"Message-Id\", \"<20160627151901.3295.78981.1336C636@sandboxbcd3ccb1a529415db665622619a61616.mailgun.org>\"], [\"To\", \"Ronny <test@snowplowanalytics.com>\"], [\"From\", \"Mailgun Sandbox <postmaster@sandboxbcd3ccb1a529415db665622619a61616.mailgun.org>\"], [\"Subject\", \"Hello Ronny\"], [\"Content-Type\", [\"text/plain\", {\"charset\": \"ascii\"}]], [\"Mime-Version\", \"1.0\"], [\"Content-Transfer-Encoding\", [\"7bit\", {}]]]","body-plain":"","timestamp":"1467040750","event":"delivered","domain":"sandboxbcd3ccb1a529415db665622619a61616.mailgun.org","signature":"9387fb0e5ff02de5e159594173f02c95c55d7e681b40a7b930ed4d0a3cbbdd6e","token":"c2fc6a36198fa651243afb6042867b7490e480843198008c6b","Message-Id":"<20160627151901.3295.78981.1336C636@sandboxbcd3ccb1a529415db665622619a61616.mailgun.org>","X-Mailgun-Sid":"WyIxZjQzMiIsICJyb25ueUBrZGUub3JnIiwgIjliMjYwIl0="}}}""",
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
class CljTomcatMailgunEventSpec extends Specification {

  "A job which processes a Clojure-Tomcat file containing a Mailgun POST raw event representing 1 valid completed call" should {
    EtlJobSpec("clj-tomcat", "2", true, List("geo")).
      source(MultipleTextLineFiles("inputFolder"), CljTomcatMailgunEventSpec.lines).
      sink[TupleEntry](Tsv("outputFolder")){ buf : Buffer[TupleEntry] =>
        "correctly output 1 completed call" in {
          buf.size must_== 1
          val actual = buf.head
          for (idx <- CljTomcatMailgunEventSpec.expected.indices) {
            actual.getString(idx) must beFieldEqualTo(CljTomcatMailgunEventSpec.expected(idx), withIndex = idx)
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
