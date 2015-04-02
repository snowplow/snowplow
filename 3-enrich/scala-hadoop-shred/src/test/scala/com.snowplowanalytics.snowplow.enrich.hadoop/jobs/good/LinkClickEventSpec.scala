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
object LinkClickEventSpec {

  val lines = Lines(
    """snowplowweb	web	2014-06-01 14:04:11.639	2014-05-29 18:16:35.000	2014-05-29 18:16:35.967	unstruct	2b1b25a4-c0df-4859-8201-cf21492ad61b	114221	clojure	js-2.0.0-M2	clj-0.6.0-tom-0.0.4	hadoop-0.5.0-common-0.4.0		68.42.204.218	1242058182	58df65c46e1ac937	11	437ad25b-2006-455e-b5d8-d664b74df8f3	US	MI	Holland	49423	42.742294	-86.0661						http://snowplowanalytics.com/blog/		https://www.google.com/	http	snowplowanalytics.com	80	/blog/			https	www.google.com	80	/			search	Google													{"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0","data":{"schema":"iglu:com.snowplowanalytics.snowplow/link_click/jsonschema/1-0-0","data":{"targetUrl":"http://snowplowanalytics.com/blog/page2","elementClasses":["next"]}}}																			Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.114 Safari/537.36	Chrome	Chrome		Browser	WEBKIT	en-US	1	1	1	0	1	0	0	0	1	1	24	1241	806	Mac OS	Mac OS	Apple Inc.	America/New_York	Computer	0	1440	900	UTF-8														"""
    )

  object expected {
    val vendor = "com.snowplowanalytics.snowplow"
    val path = s"${vendor}/link_click/jsonschema/1-0-0"
    val contents  = 
      s"""|{
            |"schema":{
              |"vendor":"com.snowplowanalytics.snowplow",
              |"name":"link_click",
              |"format":"jsonschema",
              |"version":"1-0-0"
            |},
            |"data":{
              |"targetUrl":"http://snowplowanalytics.com/blog/page2",
              |"elementClasses":["next"]
            |},
            |"hierarchy":{
              |"rootId":"2b1b25a4-c0df-4859-8201-cf21492ad61b",
              |"rootTstamp":"2014-05-29 18:16:35.000",
              |"refRoot":"events",
              |"refTree":["events","link_click"],
              |"refParent":"events"
            |}
          |}""".stripMargin.replaceAll("[\n\r]","")
  }
}

/**
 * Integration test for the EtlJob:
 *
 * The enriched event contains a JSON
 * which should pass validation.
 */
class LinkClickEventSpec extends Specification {

  "A job which is provided with a valid Snowplow link_click event" should {

    val Sinks =
      JobSpecHelpers.runJobInTool(LinkClickEventSpec.lines)

    "shred the Snowplow link_click event into its appropriate path" in {
      // TODO: move this out
      // Java
      import java.io.File
      // Scala
      import scala.io.{Source => ScalaSource}
      val linkClickEventSource = ScalaSource.fromFile(new File(Sinks.output, LinkClickEventSpec.expected.path))
      linkClickEventSource.getLines.toList mustEqual Seq(LinkClickEventSpec.expected.contents)
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
