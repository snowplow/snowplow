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
object CljTomcatMixpanelEventSpec {

  val lines = Lines(
    "2014-10-09  16:28:31    -   13  255.255.255.255   POST    255.255.255.255   /com.mixpanel/v1  404 -  -    aid=email&cv=clj-0.6.0-tom-0.0.4&nuid=-   -   -   -   application%2Fx-www-form-urlencoded   dXNlcnM9JTVCJTdCJTIyJTI0ZGlzdGluY3RfaWQlMjIlM0ElMjAlMjJzbWl0aCU0MGphbmUuY29tJTIyJTJDJTIwJTIyJTI0cHJvcGVydGllcyUyMiUzQSUyMCU3QiUyMiUyNG5hbWUlMjIlM0ElMjAlMjJTbWl0aCUyMEphbmUlMjIlMkMlMjAlMjIlMjRlbWFpbCUyMiUzQSUyMCUyMnNtaXRoJTQwamFuZS5jb20lMjIlN0QlN0QlNUQlMEE="
  )

  val expected = List(
    "email",
    "srv",
    EtlTimestamp,
    "2014-10-09 16:28:31.000",
    null,
    "unstruct",
    null,
    null,
    null,
    "com.mixpanel-v1",
    "clj-0.6.0-tom-0.0.4",
    EtlVersion,
    null,
    "255.255.x.x",
    null,
    null,
    null,
    "-",
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
    """{"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0","data":{"schema":"iglu:com.mixpanel/users/jsonschema/1-0-0","data":{"$distinct_id":"smith@jane.com","$properties":{"$name":"Smith Jane","$email":"smith@jane.com"}}}}""",
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
  * For details:com
  * https://forums.aws.amazon.com/thread.jspa?threadID=134017&tstart=0#
  */
class CljTomcatMixpanelEventSpec extends Specification {

  "A job which processes a Clojure-Tomcat file containing a Mixpanel POST with 1 raw event representing 1 valid completed call" should {
    EtlJobSpec("clj-tomcat", "2", true, List("geo")).
      source(MultipleTextLineFiles("inputFolder"), CljTomcatMixpanelEventSpec.lines).
      sink[TupleEntry](Tsv("outputFolder")){ buf : Buffer[TupleEntry] =>
      "correctly output 1 completed call" in {
        buf.size must_== 1
        val actual = buf.head
        for (idx <- CljTomcatMixpanelEventSpec.expected.indices) {
          actual.getString(idx) must beFieldEqualTo(CljTomcatMixpanelEventSpec.expected(idx), withIndex = idx)
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