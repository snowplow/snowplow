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
object MasterCljTomcatSpec {

  // Concatenate ALL lines from ALL other jobs
  val lines = good.CljTomcatTp1SingleEventSpec.lines ++  // 1 good
              good.CljTomcatCallrailEventSpec.lines ++   // 1 good
              good.CljTomcatTp2MultiEventsSpec.lines ++  // 3 good
              good.CljTomcatTp2MegaEventsSpec.lines      // 7,500 good = 7,505 GOOD

  object expected {
    val goodCount = 7505
    val badCount = 0
  }
}

/**
 * Integration test for the EtlJob:
 *
 * Master test which runs using all of the
 * individual good, bad and misc tests
 */
class MasterCljTomcatSpec extends Specification {

  "A job which processes a Clojure-Tomcat file containing 7,505 valid events, 0 bad lines and 3 discardable lines" should {
    EtlJobSpec("clj-tomcat", "1", false, List("geo")).
      source(MultipleTextLineFiles("inputFolder"), MasterCljTomcatSpec.lines).
      sink[String](Tsv("outputFolder")){ output =>
        "write 7,505 events" in {
          output.size must_== MasterCljTomcatSpec.expected.goodCount
        }
      }.
      sink[TupleEntry](Tsv("exceptionsFolder")){ trap =>
        "not trap any exceptions" in {
          trap must beEmpty
        }
      }.
      sink[String](Tsv("badFolder")){ error =>
        "write 0 bad rows" in {
          error.size must_== MasterCljTomcatSpec.expected.badCount
        }
      }.
      run.
      finish
  }
}
