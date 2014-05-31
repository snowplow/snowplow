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

// Java
import java.io.File

// Scala
import scala.io.{Source => ScalaSource}

// Scalding
import com.twitter.scalding._

// Cascading
import cascading.tuple.TupleEntry
import cascading.tap.SinkMode

// This project
import JobSpecHelpers._

// Specs2
import org.specs2.mutable.Specification

// TODO
import java.io.BufferedWriter
import java.io.FileWriter
import scala.io.Source.fromFile
import java.io.File
import cascading.cascade.Cascade
import cascading.flow.FlowSkipIfSinkNotStale
import cascading.tuple.Fields

/**
 * Holds the input data for the test,
 * plus a lambda to create the expected
 * output.
 */
object WebsitePageContextSpec {

  val lines = 
    """snowplowweb	web	2014-05-29 18:04:12.000	2014-05-29 18:04:11.639	page_view		a4583919-4df8-496a-917b-d40fa1c8ca7f	836413	clojure	js-2.0.0-M2	clj-0.6.0-tom-0.0.4	hadoop-0.5.0-common-0.4.0		216.207.42.134	3499345421	3b1d1a375044eede	3	2bad2a4e-aae4-4bea-8acd-399e7fe0366a	US	CA	South San Francisco		37.654694	-122.4077	http://snowplowanalytics.com/blog/2013/02/08/writing-hive-udfs-and-serdes/	Writing Hive UDFs - a tutorial		http	snowplowanalytics.com	80	/blog/2013/02/08/writing-hive-udfs-and-serdes/																	{"schema":"iglu://com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-0","data":[{"schema":"iglu://com.snowplowanalytics.website/page_context/jsonschema/1-0-0","data":{"author":"Alex Dean","topics":["hive","udf","serde","java","hadoop"],"subCategory":"inside the plow","category":"blog","whenPublished":"2013-02-08"}}]}																										Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.75.14 (KHTML, like Gecko) Version/7.0.3 Safari/537.75.14	Safari	Safari		Browser	WEBKIT	en-us	0	0	0	0	0	0	0	0	0	1	24	1440	1845	Mac OS	Mac OS	Apple Inc.	America/Los_Angeles	Computer	0	1440	900	UTF-8	1440	6015"""

  val expected = Seq(
    s"""xxx"""
    )

  import Dsl._
  val goodTsv = PartitionedTsv("outputFolder", ShredJob.ShreddedPartition, false, ('json), SinkMode.REPLACE)
}

/**
 * Integration test for the EtlJob:
 *
 * The enriched event contains a JSON
 * which should pass validation.
 */
class WebsitePageContextSpec extends Specification {

  import Dsl._

  "A job which is provided with a valid custom context" should {

    "shred the custom context into its appropriate path" in {

    val input = File.createTempFile("snowplow-shred-job-input-", "")
    input.mkdirs()

    val writer = new BufferedWriter(new FileWriter(input))
    writer.write(WebsitePageContextSpec.lines)
    writer.close()

    val badRows = File.createTempFile("snowplow-shred-job-bad-rows-", "")
    badRows.mkdir()

    val output = File.createTempFile("snowplow-shred-job-output-", "")
    output.mkdir()

    val args = Array[String]("com.snowplowanalytics.snowplow.enrich.hadoop.ShredJob", "--local",
      "--input_folder", input.getAbsolutePath,
      "--output_folder", output.getAbsolutePath,
      "--bad_rows_folder", badRows.getAbsolutePath)

    Tool.main(args)

    output.listFiles().map({ _.getName() }).toSet mustEqual Set("com.snowplowanalytics.website")

    // val pageContextSource = ScalaSource.fromFile(new File(output, "com.snowplowanalytics.website/ad_click/jsonschema/1-0-0/part-00000-00000"))

    // pageContextSource.getLines.toList mustEqual Seq("""{ "event": "ad_click" } """)

/*


      val directory = new File(testMode.getWritePathFor(WebsitePageContextSpec.goodTsv))

      directory.listFiles().map({ _.getName() }).toSet mustEqual Set("com.snowplowanalytics.snowplow", "com.zendesk")

      val adClickSource = ScalaSource.fromFile(new File(directory, "com.snowplowanalytics.snowplow/ad_click/jsonschema/1-0-0/part-00000-00000"))
      val newTicketSource = ScalaSource.fromFile(new File(directory, "com.zendesk/new-ticket/jsonschema/1-1-0/part-00000-00001"))

      adClickSource.getLines.toList mustEqual Seq("""{ "event": "ad_click" } """)
      newTicketSource.getLines.toList mustEqual Seq("""{ "event": "new-ticket" } """) */

      // 1 must_== 1
    }
  }
}
