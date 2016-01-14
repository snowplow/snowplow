/*
 * Copyright (c) 2014 Snowplow Analytics Ltd. All rights reserved.
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
package good

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
 * plus the expected output.
 */
object DerivedContextsSpec {

  // Contexts schema version 1-0-1
  val lines = Lines(
    """snowplowweb	web	2014-06-01 14:04:11.639	2014-05-29 18:16:35.000	2014-05-29 18:04:11.639	page_view	2b1b25a4-c0df-4859-8201-cf21492ad61b	836413	clojure	js-2.0.0-M2	clj-0.6.0-tom-0.0.4	hadoop-0.5.0-common-0.4.0		216.207.42.134	3499345421	3b1d1a375044eede	3	2bad2a4e-aae4-4bea-8acd-399e7fe0366a	US	CA	South San Francisco		37.654694	-122.4077						http://snowplowanalytics.com/blog/2013/02/08/writing-hive-udfs-and-serdes/	Writing Hive UDFs - a tutorial		http	snowplowanalytics.com	80	/blog/2013/02/08/writing-hive-udfs-and-serdes/																																										Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.75.14 (KHTML, like Gecko) Version/7.0.3 Safari/537.75.14	Safari	Safari		Browser	WEBKIT	en-us	0	0	0	0	0	0	0	0	0	1	24	1440	1845	Mac OS	Mac OS	Apple Inc.	America/Los_Angeles	Computer	0	1440	900	UTF-8	1440	6015															{"schema":"iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-1","data":[{"schema":"iglu:org.schema/WebPage/jsonschema/1-0-0","data":{"datePublished":"2014-07-23T00:00:00Z","author":"Jonathan Almeida","inLanguage":"en-US","genre":"blog","breadcrumb":["blog","releases"],"keywords":["snowplow","analytics","java","jvm","tracker"]}}]}"""
    )

  object expected {
    val vendor = "org.schema"
    val path = s"${vendor}/WebPage/jsonschema/1-0-0"
    val contents  = 
      s"""|{
            |"schema":{
              |"vendor":"org.schema",
              |"name":"WebPage",
              |"format":"jsonschema",
              |"version":"1-0-0"
            |},
            |"data":{
              |"datePublished":"2014-07-23T00:00:00Z",
              |"author":"Jonathan Almeida",
              |"inLanguage":"en-US",
              |"genre":"blog",
              |"breadcrumb":["blog","releases"],
              |"keywords":["snowplow","analytics","java","jvm","tracker"]
            |},
            |"hierarchy":{
              |"rootId":"2b1b25a4-c0df-4859-8201-cf21492ad61b",
              |"rootTstamp":"2014-05-29 18:16:35.000",
              |"refRoot":"events",
              |"refTree":["events","WebPage"],
              |"refParent":"events"
            |}
          |}""".stripMargin.replaceAll("[\n\r]","")

    // Removed three JSON columns and added 7 columns at the end
    val event = """snowplowweb	web	2014-06-01 14:04:11.639	2014-05-29 18:16:35.000	2014-05-29 18:04:11.639	page_view	2b1b25a4-c0df-4859-8201-cf21492ad61b	836413	clojure	js-2.0.0-M2	clj-0.6.0-tom-0.0.4	hadoop-0.5.0-common-0.4.0		216.207.42.134	3499345421	3b1d1a375044eede	3	2bad2a4e-aae4-4bea-8acd-399e7fe0366a	US	CA	South San Francisco		37.654694	-122.4077						http://snowplowanalytics.com/blog/2013/02/08/writing-hive-udfs-and-serdes/	Writing Hive UDFs - a tutorial		http	snowplowanalytics.com	80	/blog/2013/02/08/writing-hive-udfs-and-serdes/																																								Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.75.14 (KHTML, like Gecko) Version/7.0.3 Safari/537.75.14	Safari	Safari		Browser	WEBKIT	en-us	0	0	0	0	0	0	0	0	0	1	24	1440	1845	Mac OS	Mac OS	Apple Inc.	America/Los_Angeles	Computer	0	1440	900	UTF-8	1440	6015																						"""
  }
}

/**
 * Integration test for the EtlJob:
 *
 * The enriched event contains a JSON
 * which should pass validation.
 */
class DerivedContextsSpec extends Specification {

  "A job which is provided with a valid website page_context" should {

    val Sinks =
      JobSpecHelpers.runJobInTool(DerivedContextsSpec.lines)

    "transform the enriched event and store it in /atomic-events" in {
      val lines = JobSpecHelpers.readFile(Sinks.output, "atomic-events")
      lines mustEqual Seq(DerivedContextsSpec.expected.event)
    }
    "shred the website page_context into its appropriate path" in {
      val lines = JobSpecHelpers.readFile(Sinks.output, DerivedContextsSpec.expected.path)
      lines mustEqual Seq(DerivedContextsSpec.expected.contents)
    }

    "not shred any unexpected JSONs" in {
      1 must_== 1 // TODO
    }
    "not trap any exceptions" in {
      // TODO: not working
      Sinks.exceptions must beEmptyDir
    }
    "not write any bad row JSONs" in {
      // TODO: not working
      Sinks.badRows must beEmptyDir
    }

    // Sinks.deleteAll()
    ()
  }
}
