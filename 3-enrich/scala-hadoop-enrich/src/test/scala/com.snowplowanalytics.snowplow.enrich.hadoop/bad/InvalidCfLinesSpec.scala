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
 * Holds the input data for the test,
 * plus a lambda to create the expected
 * output.
 */
object InvalidCfLinesSpec {

  val lines = Lines(
    "",
    "NOT VALID",
    "2012-05-21  07:14:47  FRA2  3343  83.4.209.35 GET d3t05xllj8hhgj.cloudfront.net"
    )

  val expected = (line: String) => """{"line":"%s","errors":["Line does not match CloudFront header or data row formats"]}""".format(line)
}

/**
 * Integration test for the EtlJob:
 *
 * Input data _is_ not in the
 * expected CloudFront format.
 */
class InvalidCfLinesSpec extends Specification with TupleConversions {

  "A job which processes input lines not in CloudFront format" should {
    EtlJobSpec("cloudfront", "0").
      source(MultipleTextLineFiles("inputFolder"), InvalidCfLinesSpec.lines).
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
      sink[String](JsonLine("badFolder")){ json =>
        "write a bad row JSON with input line and error message for each input line" in {
          for (i <- json.indices) {
            json(i) must_== InvalidCfLinesSpec.expected(InvalidCfLinesSpec.lines(i)._2)
          }
        }
      }.
      run.
      finish
  }
}