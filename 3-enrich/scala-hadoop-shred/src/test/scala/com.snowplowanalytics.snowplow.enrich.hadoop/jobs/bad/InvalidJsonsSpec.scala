/*
 * Copyright (c) 2014-2015 Snowplow Analytics Ltd. All rights reserved.
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
package jobs
package bad

// Scalding
import com.twitter.scalding._

// Cascading
import cascading.tuple.TupleEntry
import cascading.tap.SinkMode

// This project
import JobSpecHelpers._

// Specs2
import org.specs2.mutable.Specification

/**
 * Holds the input data for the test,
 * plus a lambda to create the expected
 * output.
 */
object InvalidJsonsSpec {

  val lines = Lines(
    """snowplowweb	web	2014-06-01 14:04:11.639	2014-05-29 18:04:12.000	2014-05-29 18:04:11.639	page_view	a4583919-4df8-496a-917b-d40fa1c8ca7f	836413	clojure	js-2.0.0-M2	clj-0.6.0-tom-0.0.4	hadoop-0.5.0-common-0.4.0		216.207.42.134	3499345421	3b1d1a375044eede	3	2bad2a4e-aae4-4bea-8acd-399e7fe0366a	US	CA	South San Francisco		37.654694	-122.4077						http://snowplowanalytics.com/blog/2013/02/08/writing-hive-udfs-and-serdes/	Writing Hive UDFs - a tutorial		http	snowplowanalytics.com	80	/blog/2013/02/08/writing-hive-udfs-and-serdes/																	&&&						|%|																			Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.75.14 (KHTML, like Gecko) Version/7.0.3 Safari/537.75.14	Safari	Safari		Browser	WEBKIT	en-us	0	0	0	0	0	0	0	0	0	1	24	1440	1845	Mac OS	Mac OS	Apple Inc.	America/Los_Angeles	Computer	0	1440	900	UTF-8	1440	6015"""
    )

  val expected = (line: String) =>
    """{"line":"%s","errors":[{"level":"error","message":"Field [ue_properties]: invalid JSON [|%%|] with parsing error: Unexpected character ('|' (code 124)): expected a valid value (number, String, array, object, 'true', 'false' or 'null') at [Source: java.io.StringReader@xxxxxx; line: 1, column: 2]"},{"level":"error","message":"Field [context]: invalid JSON [&&&] with parsing error: Unexpected character ('&' (code 38)): expected a valid value (number, String, array, object, 'true', 'false' or 'null') at [Source: java.io.StringReader@xxxxxx; line: 1, column: 2]"}]}"""
      .format(line)
      .replaceAll("[\t]","\\\\t")
}

/**
 * Integration test for the EtlJob:
 *
 * The JSON Schema for the context
 * cannot be located.
 */
class InvalidJsonsSpec extends Specification {

  import Dsl._

  "A job which contains invalid JSONs" should {
    ShredJobSpec.
      source(MultipleTextLineFiles("inputFolder"), InvalidJsonsSpec.lines).
      source(MultipleTextLineFiles("outputFolder/atomic-events"), InvalidJsonsSpec.lines).
      sink[String](PartitionedTsv("outputFolder", ShredJob.ShreddedPartition, false, ('json), SinkMode.REPLACE)){ output =>
        "not write any events" in {
          output must beEmpty
        }
      }.
      sink[TupleEntry](Tsv("exceptionsFolder")){ trap =>
        "not trap any exceptions" in {
          trap must beEmpty
        }
      }.
      sink[String](Tsv("badFolder")){ json =>
        "write a bad row JSON with input line and error message for each bad JSON" in {
          for (i <- json.indices) {
            removeTstamp(json(i)) must_== InvalidJsonsSpec.expected(InvalidJsonsSpec.lines(i)._2)
          }
        }
      }.
      run.
      finish
  }
}
