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
import JobSpecHelpers._

/**
 * Holds the input and expected output data
 * (counts) for the test.
 */
object MasterCfLinesSpec {

  // Concatenate ALL lines from ALL other jobs
  val lines = bad.BadTrackerCfLinesSpec.lines ++      // 3 bad
              bad.CorruptedCfLinesSpec.lines ++       // 1 bad
              bad.InvalidCfLinesSpec.lines ++         // 3 bad  = 7 BAD
              good.Aug2013CfLineSpec.lines ++         // 1 good
              good.Sep2013CfLineSpec.lines ++         // 1 good
              good.Oct2013CfLineSpec.lines ++         // 1 good
              good.LateOct2013CfLineSpec.lines ++     // 1 good
              good.Apr2014CfLineSpec.lines ++         // 1 good
              good.FutureCfLineSpec.lines ++          // 1 good
              good.CljTomcatLineSpec.lines ++         // 1 good
              good.PagePingCfLineSpec.lines ++        // 1 good
              good.PageViewCfLineSpec.lines ++        // 1 good
              good.StructEventCfLineSpec.lines ++     // 1 good
              good.UnstructEventCfLineSpec.lines ++   // 1 good
              good.TransactionCfLineSpec.lines ++     // 1 good
              good.TransactionItemCfLineSpec.lines ++ // 1 good = 13 GOOD
              misc.DiscardableCfLinesSpec.lines       // 3 discarded

  object expected {
    val goodCount = 13
    val badCount = 7
  }
}

/**
 * Integration test for the EtlJob:
 *
 * Master test which runs using all of the
 * individual good, bad and misc tests
 */
class MasterCfLinesSpec extends Specification with TupleConversions {

  "A job which processes a CloudFront file containing 13 valid events, 6 bad lines and 3 discardable lines" should {
    EtlJobSpec("cloudfront", "0"). // Technically CljTomcatLineSpec isn't CloudFront format but won't break this test
      source(MultipleTextLineFiles("inputFolder"), MasterCfLinesSpec.lines).
      sink[String](Tsv("outputFolder")){ output =>
        "write 13 events" in {
          output.size must_== MasterCfLinesSpec.expected.goodCount
        }
      }.
      sink[TupleEntry](Tsv("exceptionsFolder")){ trap =>
        "not trap any exceptions" in {
          trap must beEmpty
        }
      }.
      sink[String](JsonLine("badFolder")){ error =>
        "write 7 bad rows" in {
          error.size must_== MasterCfLinesSpec.expected.badCount
        }
      }.
      run.
      finish
  }
}