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
object ShredTestSpec {

  // April 2014: Amazon added a new field (time-taken) to the end of the Access Log format
  val lines = Lines(
    "hack hack hack and hack"
    )

  val expected = List(
    "hack hack hack and hack"
    )
}

/**
 * Integration test for the ShredJob:
 *
 * XXX
 */
class ShredTestSpec extends Specification {

  "A ShredJob which processes a Snowplow enriched event TSV file containing XXX" should {
    ShredJobSpec.
      source(MultipleTextLineFiles("inputFolder"), ShredTestSpec.lines).
      sink[(String,Int)](Tsv("outputFolder")){ buf =>
        "count words correctly" in {
          val outMap = buf.toMap
          outMap("hack") must be_==(4)
          outMap("and") must be_==(1)
        }
      }.
      sink[TupleEntry](Tsv("exceptionsFolder")){ trap =>
        "not trap any exceptions" in {
          trap must beEmpty
        }
      }.
      sink[String](JsonLine("badFolder")){ error =>
        "not write any bad rows" in {
          error must beEmpty
        }
      }.
      run.
      finish
  }
}