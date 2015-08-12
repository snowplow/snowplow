/*
 * Copyright (c) 2014 Snowplow Analytics Ltd. All rights reserved.
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
package jobs
package bad

// Scalding
import com.twitter.scalding._

// Cascading
import cascading.tuple.TupleEntry
import cascading.tap.SinkMode

// This project
import JobSpecHelpers._

// Specs2
import org.specs2.mutable.Specification

/**
 * Holds the input data for the test,
 * plus a lambda to create the expected
 * output.
 */
object NotEnrichedEventsSpec {

  val lines = Lines(
    "",
    "NOT AN ENRICHED EVENT",
    "2012-05-21  07:14:47  FRA2  3343  83.4.209.35 GET d3t05xllj8hhgj.cloudfront.net"
    )

  val expected = (line: String) => """{"line":"%s","errors":["error: Line does not match Snowplow enriched event (expected 108+ fields; found 1)\n    level: \"error\"\n"]}"""
    .format(line)
}

/**
 * Integration test for the EtlJob:
 *
 * Input data is _not_ in the expected
 * Snowplow enriched event format.
 */
class NotEnrichedEventsSpec extends Specification {

  import Dsl._

  "A job which processes input lines not containing Snowplow enriched events" should {
    ShredJobSpec.
      source(MultipleTextLineFiles("inputFolder"), NotEnrichedEventsSpec.lines).
      sink[String](PartitionedTsv("outputFolder", ShredJob.ShreddedPartition, false, ('json), SinkMode.REPLACE)){ output =>
        "not write any events" in {
          output must beEmpty
        }
      }.
      sink[TupleEntry](Tsv("exceptionsFolder")){ trap =>
        "not trap any exceptions" in {
          trap must beEmpty
        }
      }.
      sink[String](Tsv("badFolder")){ json =>
        "write a bad row JSON with input line and error message for each input line" in {
          for (i <- json.indices) {
            removeTstamp(json(i)) must_== NotEnrichedEventsSpec.expected(NotEnrichedEventsSpec.lines(i)._2)
          }
        }
      }.
      run.
      finish
  }
}
