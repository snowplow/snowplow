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
package bad

// Specs2
import org.specs2.mutable.Specification

// Scalding
import com.twitter.scalding._

// Cascading
import cascading.tuple.TupleEntry

// This project
import JobSpecHelpers._

/**
 * Holds the input and expected output data
 * for the test.
 */
object CorruptedThriftLinesSpec {

  val lines = List(
    "bad".getBytes -> 1L
    )

  val expected = """{"line":"YmFk","errors":["error: Error deserializing raw event: Cannot read. Remote side has closed. Tried to read 1 bytes, but only got 0 bytes. (This is often indicative of an internal error on the server side. Please check your server logs.)\n    level: \"error\"\n"]}"""
}

/**
 * Integration test for the EtlJob:
 *
 * Input Thrift data cannot be decoded so should be base 64 encoded in the resulting bad row.
 */
class CorruptedThriftLinesSpec extends Specification {

  "A job which processes a corrupted input line" should {
    EtlJobSpec("thrift", "1", false, List("geo")).
      source(FixedPathLzoRaw("inputFolder"), CorruptedThriftLinesSpec.lines).
      sink[String](Tsv("outputFolder")){ output =>
        "not write any events" in {
          output must beEmpty
        }
      }.
      sink[TupleEntry](Tsv("exceptionsFolder")){ trap =>
        "not trap any exceptions" in {
          trap must beEmpty
        }
      }.
      sink[String](Tsv("badFolder")){ buf =>
        val json = buf.head
        "write a bad row JSON containing the input line and all errors" in {
          removeTstamp(json) must_== CorruptedThriftLinesSpec.expected
        }
      }.
      run.
      finish
  }
}
