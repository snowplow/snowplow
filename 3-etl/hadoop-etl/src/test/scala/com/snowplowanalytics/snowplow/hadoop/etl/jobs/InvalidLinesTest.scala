/*
 * Copyright (c) 2012-2013 SnowPlow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.hadoop.etl
package jobs

// Specs2
import org.specs2.mutable.Specification

// Scalding
import com.twitter.scalding._

// This project
import JobTestHelpers._

/**
 * Integration test for the EtlJob:
 *
 * Input data _is_ not in the
 * expected CloudFront format.
 */
class InvalidLinesTest extends Specification with TupleConversions {

  "A job which processes input lines not in CloudFront format" should {
    "write an error JSON with input line and error message for each input line" in {

    	val badLines = Lines(
        "",
        "NOT VALID",
        "2012-05-21  07:14:47  FRA2  3343  83.4.209.35 GET d3t05xllj8hhgj.cloudfront.net"
        )

      val expected = (line: String) => """{"line":"%s","errors":["Line does not match CloudFront header or data row formats"]}""".format(line)

      EtlJobTest.
        source(MultipleTextLineFiles("inputFolder"), badLines).
        sink[String](Tsv("outputFolder")){ output => output must beEmpty }.
        sink[String](JsonLine("errorFolder")){ json =>
          for (i <- json.indices)
            json(i) must_== expected(badLines(i)._2)
        }.
        run.
        finish
    }
  }
}