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
object CljTomcatOlarkEventSpec {

  val lines = Lines(
    "2014-10-09  16:28:31    -   13  255.255.255.255   POST    255.255.255.255   /com.olark/v1   404 -  -    aid=email&cv=clj-0.6.0-tom-0.0.4&nuid=-   -   -   -   application%2Fx-www-form-urlencoded   bWFuZHJpbGxfZXZlbnRzPSU1QiUwQSUyMCUyMCUyMCUyMCU3QiUwQSUyMCUyMCUyMCUyMCUyMCUyMCUyMCUyMCUyMmV2ZW50JTIyJTNBJTIwJTIyc2VuZCUyMiUyQyUwQSUyMCUyMCUyMCUyMCUyMCUyMCUyMCUyMCUyMm1zZyUyMiUzQSUyMCU3QiUwQSUyMCUyMCUyMCUyMCUyMCUyMCUyMCUyMCUyMCUyMCUyMCUyMCUyMnRzJTIyJTNBJTIwMTM2NTEwOTk5OSUyQyUwQSUyMCUyMCUyMCUyMCUyMCUyMCUyMCUyMCUyMCUyMCUyMCUyMCUyMnN1YmplY3QlMjIlM0ElMjAlMjJUaGlzJTIwYW4lMjBleGFtcGxlJTIwd2ViaG9vayUyMG1lc3NhZ2UlMjIlMkMlMEElMjAlMjAlMjAlMjAlMjAlMjAlMjAlMjAlMjAlMjAlMjAlMjAlMjJlbWFpbCUyMiUzQSUyMCUyMmV4YW1wbGUud2ViaG9vayU0MG1hbmRyaWxsYXBwLmNvbSUyMiUyQyUwQSUyMCUyMCUyMCUyMCUyMCUyMCUyMCUyMCUyMCUyMCUyMCUyMCUyMnNlbmRlciUyMiUzQSUyMCUyMmV4YW1wbGUuc2VuZGVyJTQwbWFuZHJpbGxhcHAuY29tJTIyJTJDJTBBJTIwJTIwJTIwJTIwJTIwJTIwJTIwJTIwJTIwJTIwJTIwJTIwJTIydGFncyUyMiUzQSUyMCU1QiUwQSUyMCUyMCUyMCUyMCUyMCUyMCUyMCUyMCUyMCUyMCUyMCUyMCUyMCUyMCUyMCUyMCUyMndlYmhvb2stZXhhbXBsZSUyMiUwQSUyMCUyMCUyMCUyMCUyMCUyMCUyMCUyMCUyMCUyMCUyMCUyMCU1RCUyQyUwQSUyMCUyMCUyMCUyMCUyMCUyMCUyMCUyMCUyMCUyMCUyMCUyMCUyMm9wZW5zJTIyJTNBJTIwJTVCJTVEJTJDJTBBJTIwJTIwJTIwJTIwJTIwJTIwJTIwJTIwJTIwJTIwJTIwJTIwJTIyY2xpY2tzJTIyJTNBJTIwJTVCJTVEJTJDJTBBJTIwJTIwJTIwJTIwJTIwJTIwJTIwJTIwJTIwJTIwJTIwJTIwJTIyc3RhdGUlMjIlM0ElMjAlMjJzZW50JTIyJTJDJTBBJTIwJTIwJTIwJTIwJTIwJTIwJTIwJTIwJTIwJTIwJTIwJTIwJTIybWV0YWRhdGElMjIlM0ElMjAlN0IlMEElMjAlMjAlMjAlMjAlMjAlMjAlMjAlMjAlMjAlMjAlMjAlMjAlMjAlMjAlMjAlMjAlMjJ1c2VyX2lkJTIyJTNBJTIwMTExJTBBJTIwJTIwJTIwJTIwJTIwJTIwJTIwJTIwJTIwJTIwJTIwJTIwJTdEJTJDJTBBJTIwJTIwJTIwJTIwJTIwJTIwJTIwJTIwJTIwJTIwJTIwJTIwJTIyX2lkJTIyJTNBJTIwJTIyZXhhbXBsZWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWElMjIlMkMlMEElMjAlMjAlMjAlMjAlMjAlMjAlMjAlMjAlMjAlMjAlMjAlMjAlMjJfdmVyc2lvbiUyMiUzQSUyMCUyMmV4YW1wbGVhYWFhYWFhYWFhYWFhYWElMjIlMEElMjAlMjAlMjAlMjAlMjAlMjAlMjAlMjAlN0QlMkMlMEElMjAlMjAlMjAlMjAlMjAlMjAlMjAlMjAlMjJfaWQlMjIlM0ElMjAlMjJleGFtcGxlYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYSUyMiUyQyUwQSUyMCUyMCUyMCUyMCUyMCUyMCUyMCUyMCUyMnRzJTIyJTNBJTIwMTQxNTY5MjAzNSUwQSUyMCUyMCUyMCUyMCU3RCUwQSU1RA=="
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
    "com.olark-v1",
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
    """{"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0","data":{"schema":"iglu:com.olark/transcript/jsonschema/1-0-0","data":{"kind":"Conversation","tags":["olark","customer"],"items":[{"body":"Hi there. Need any help?","timestamp":"1307116657.1","kind":"MessageToVisitor","nickname":"John","operatorId":"1234"},{"body":"Yes, please help me with billing.","timestamp":"1307116661.25","kind":"MessageToOperator","nickname":"Bob"}],"operators":{"1234":{"username":"jdoe","emailAddress":"john@example.com","kind":"Operator","nickname":"John","id":"1234"} },"groups":[{"kind":"Group","name":"My Sales Group","id":"0123456789abcdef"}],"visitor":{"ip":"123.4.56.78","city":"Palo Alto","kind":"Visitor","conversationBeginPage":"http://www.example.com/path","countryCode":"US","country":"United State","region":"CA","chat_feedback":{"overall_chat":5,"responsiveness":5,"friendliness":5,"knowledge":5,"comments":"Very helpful, thanks"},"operatingSystem":"Windows","emailAddress":"bob@example.com","organization":"Widgets Inc.","phoneNumber":"(555) 555-5555","fullName":"Bob Doe","customFields":{"favoriteColor":"blue","myInternalCustomerId":"12341234"},"id":"9QRF9YWM5XW3ZSU7P9CGWRU89944341","browser":"Chrome 12.1"},"id":"EV695BI2930A6XMO32886MPT899443414"}}}""",
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
class CljTomcatOlarkEventSpec extends Specification {

  "A job which processes a Clojure-Tomcat file containing an Olark POST raw event representing 1 valid completed call" should {
    EtlJobSpec("clj-tomcat", "2", true, List("geo")).
      source(MultipleTextLineFiles("inputFolder"), CljTomcatOlarkEventSpec.lines).
      sink[TupleEntry](Tsv("outputFolder")){ buf : Buffer[TupleEntry] =>
        "correctly output 1 completed call" in {
          buf.size must_== 1
          val actual = buf.head
          for (idx <- CljTomcatOlarkEventSpec.expected.indices) {
            actual.getString(idx) must beFieldEqualTo(CljTomcatOlarkEventSpec.expected(idx), withIndex = idx)
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
