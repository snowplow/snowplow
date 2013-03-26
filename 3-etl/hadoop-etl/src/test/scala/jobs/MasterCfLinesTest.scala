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

// Cascading
import cascading.tuple.TupleEntry

// This project
import JobTestHelpers._

/**
 * Holds the input for the test.
 */
object MasterCfLinesTest {

  // Concatenate ALL lines from ALL other jobs
  val lines = bad.BadTrackerCfLinesTest.lines ++
              bad.CorruptedCfLinesTest.lines ++
              bad.InvalidCfLinesTest.lines ++
              good.PagePingCfLineTest.lines ++
              good.PageViewCfLineTest.lines ++
              good.StructEventCfLineTest.lines ++
              good.TransactionCfLineTest.lines ++
              good.TransactionItemCfLineTest.lines ++
              misc.DiscardableCfLinesTest.lines
}

/**
 * Integration test for the EtlJob:
 *
 * Master test which runs using all of the
 * individual good, bad and misc tests
 */
class MasterCfLinesTest extends Specification with TupleConversions {

  "A job which processes a CloudFront file containing 5 valid events, 6 bad lines and 3 discardable lines" should {
    EtlJobTest.
      source(MultipleTextLineFiles("inputFolder"), MasterCfLinesTest.lines).
      sink[String](Tsv("outputFolder")){ output =>
        "write 5 events" in {
          output.size must_== 5
        }
      }.
      sink[TupleEntry](Tsv("exceptionsFolder")){ trap =>
        "not trap any exceptions" in {
          trap must beEmpty
        }
      }.
      sink[String](JsonLine("badFolder")){ error =>
        "write 6 bad rows" in {
          error.size must_== 6
        }
      }.
      run.
      finish
  }
}