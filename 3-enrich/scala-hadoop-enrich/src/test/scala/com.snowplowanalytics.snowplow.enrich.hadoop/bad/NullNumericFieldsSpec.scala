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
object NullNumericFieldsSpec {

  val lines = Lines(
    "2014-10-11  14:01:05    -   37  172.31.38.31    GET 24.209.95.109   /i  200 http://www.myvideowebsite.com/embed/ab123456789?auto_start=e9&rf=cb Mozilla%2F5.0+%28Macintosh%3B+Intel+Mac+OS+X+10.6%3B+rv%3A32.0%29+Gecko%2F20100101+Firefox%2F32.0   e=se&se_ca=video-player%3Anewformat&se_ac=play-time&se_la=efba3ef384&se_va=&tid="
    )

  val expected = """{"line":"2014-10-11  14:01:05    -   37  172.31.38.31    GET 24.209.95.109   /i  200 http://www.myvideowebsite.com/embed/ab123456789?auto_start=e9&rf=cb Mozilla%2F5.0+%28Macintosh%3B+Intel+Mac+OS+X+10.6%3B+rv%3A32.0%29+Gecko%2F20100101+Firefox%2F32.0   e=se&se_ca=video-player%3Anewformat&se_ac=play-time&se_la=efba3ef384&se_va=&tid=","errors":["Field [se_va]: cannot convert [] to Double-like String","Field [tid]: [] is not a valid integer"]}"""
}

/**
 * Integration test for the EtlJob:
 *
 * Check that all tuples in a custom structured event
 * (CloudFront format) are successfully extracted.
 */
class NullNumericFieldsSpec extends Specification {

  "A job which processes a CloudFront file containing 1 event with null integer and double fields" should {
    EtlJobSpec("clj-tomcat", "2", true, List("geo", "organization")).
      source(MultipleTextLineFiles("inputFolder"), NullNumericFieldsSpec.lines).
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
          json must_== NullNumericFieldsSpec.expected
        }
      }.
      run.
      finish
  }
}