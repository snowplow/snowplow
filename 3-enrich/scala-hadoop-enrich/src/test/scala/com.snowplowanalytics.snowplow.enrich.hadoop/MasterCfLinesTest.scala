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

// Specs2
import org.specs2.mutable.Specification

// Scalding
import com.twitter.scalding._

// Cascading
import cascading.tuple.TupleEntry

// This project
import JobTestHelpers._

/**
 * Holds the input and expected output data
 * (counts) for the test.
 */
object MasterCfLinesTest {

  // Concatenate ALL lines from ALL other jobs
  val lines = bad.BadTrackerCfLinesTest.lines ++      // 2 bad
              bad.CorruptedCfLinesTest.lines ++       // 1 bad
              bad.InvalidCfLinesTest.lines ++         // 3 bad  = 6 BAD
              good.Aug2013CfLineTest.lines ++         // 1 good
              good.Sep2013CfLineTest.lines ++         // 1 good
              good.Oct2013CfLineTest.lines ++         // 1 good
              good.LateOct2013CfLineTest.lines ++     // 1 good
              good.CljTomcatLineTest.lines ++         // 1 good
              good.PagePingCfLineTest.lines ++        // 1 good
              good.PageViewCfLineTest.lines ++        // 1 good
              good.StructEventCfLineTest.lines ++     // 1 good
              good.TransactionCfLineTest.lines ++     // 1 good
              good.TransactionItemCfLineTest.lines ++ // 1 good = 10 GOOD
              misc.DiscardableCfLinesTest.lines       // 3 discarded

  object expected {
    val goodCount = 10
    val badCount = 6
  }
}

/**
 * Integration test for the EtlJob:
 *
 * Master test which runs using all of the
 * individual good, bad and misc tests
 */
class MasterCfLinesTest extends Specification {

  "A job which processes a CloudFront file containing 10 valid events, 6 bad lines and 3 discardable lines" should {
    EtlJobTest("cloudfront", "0"). // Technically CljTomcatLineTest isn't CloudFront format but won't break this test
      source(MultipleTextLineFiles("inputFolder"), MasterCfLinesTest.lines).
      sink[String](Tsv("outputFolder")){ output =>
        "write 10 events" in {
          output.size must_== MasterCfLinesTest.expected.goodCount
        }
      }.
      sink[TupleEntry](Tsv("exceptionsFolder")){ trap =>
        "not trap any exceptions" in {
          trap must beEmpty
        }
      }.
      sink[String](JsonLine("badFolder")){ error =>
        "write 6 bad rows" in {
          error.size must_== MasterCfLinesTest.expected.badCount
        }
      }.
      run.
      finish
  }
}