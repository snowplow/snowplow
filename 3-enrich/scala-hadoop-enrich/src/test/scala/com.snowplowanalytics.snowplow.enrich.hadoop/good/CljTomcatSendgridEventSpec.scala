/*
 * Copyright (c) 2015 Snowplow Analytics Ltd. All rights reserved.
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
object CljTomcatSendgridEventSpec {

  val lines = Lines(
    "2014-10-09  16:28:31    -   13  255.255.255.255   POST    255.255.255.255   /com.sendgrid/v3   404 -  -    aid=email&cv=clj-0.6.0-tom-0.0.4&nuid=-   -   -   -   application%2Fjson   W3siZW1haWwiOiJleGFtcGxlQHRlc3QuY29tIiwidGltZXN0YW1wIjoxNDQ2NTQ5NjE1LCJzbXRwLWlkIjoiXHUwMDNjMTRjNWQ3NWNlOTMuZGZkLjY0YjQ2OUBpc210cGQtNTU1XHUwMDNlIiwiZXZlbnQiOiJwcm9jZXNzZWQiLCJjYXRlZ29yeSI6ImNhdCBmYWN0cyIsInNnX2V2ZW50X2lkIjoic1pST3dNR01hZ0Znbk9FbVNkdmhpZz09Iiwic2dfbWVzc2FnZV9pZCI6IjE0YzVkNzVjZTkzLmRmZC42NGI0NjkuZmlsdGVyMDAwMS4xNjY0OC41NTE1RTBCODguMCJ9XQ=="
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
    "com.sendgrid-v3",
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
    """{"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0","data":{"schema":"iglu:com.sendgrid/processed/jsonschema/1-0-0","data":{"email":"example@test.com","timestamp":"2015-11-03T11:20:15.000Z","smtp-id":"\u003c14c5d75ce93.dfd.64b469@ismtpd-555\u003e","category":"cat facts","sg_event_id":"sZROwMGMagFgnOEmSdvhig==","sg_message_id":"14c5d75ce93.dfd.64b469.filter0001.16648.5515E0B88.0"}}}""",
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
class CljTomcatSendgridEventSpec extends Specification {

  "A job which processes a Clojure-Tomcat file containing a Sendgrid POST raw event representing 1 valid completed call" should {
    EtlJobSpec("clj-tomcat", "2", true, List("geo")).
      source(MultipleTextLineFiles("inputFolder"), CljTomcatSendgridEventSpec.lines).
      sink[TupleEntry](Tsv("outputFolder")){ buf : Buffer[TupleEntry] =>
      "correctly output 1 completed call" in {
        buf.size must_== 1
        val actual = buf.head
        for (idx <- CljTomcatSendgridEventSpec.expected.indices) {
          actual.getString(idx) must beFieldEqualTo(CljTomcatSendgridEventSpec.expected(idx), withIndex = idx)
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
