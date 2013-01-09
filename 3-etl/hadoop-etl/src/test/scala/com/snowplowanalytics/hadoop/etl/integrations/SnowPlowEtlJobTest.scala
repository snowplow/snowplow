/*
 * Copyright (c) 2012 Twitter, Inc.
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
package com.snowplowanalytics.snowplow.hadoop.etl.integrations

// Specs2
import org.specs2.mutable.Specification

// Scalding
import com.twitter.scalding._

/**
 * Integration test for SnowPlowEtlJob:
 *
 * placeholder that needs updating.
 */
class SnowPlowEtlJobTest extends Specification with TupleConversions {

  "A WordCount job" should {
      "count words correctly" in {

        JobTest("com.snowplowanalytics.snowplow.hadoop.etl.SnowPlowEtlJob").
          arg("input", "inputFile").
          arg("collector_format", "cloudfront").
          arg("output", "outputFile").
          source(TextLine("inputFile"), List("0" -> "hack hack hack and hack")).
          sink[(String,Int)](Tsv("outputFile")){ outputBuffer =>
            val outMap = outputBuffer.toMap
            outMap("hack") must_== 4
            outMap("and") must_== 1
          }.
          run.
          finish
          success
      }
  }
}