/*
 * copyright (c) 2012-2017 snowplow analytics ltd. all rights reserved.
 *
 * this program is licensed to you under the apache license version 2.0,
 * and you may not use this file except in compliance with the apache license version 2.0.
 * you may obtain a copy of the apache license version 2.0 at
 * http://www.apache.org/licenses/license-2.0.
 *
 * unless required by applicable law or agreed to in writing,
 * software distributed under the apache license version 2.0 is distributed on an
 * "as is" basis, without warranties or conditions of any kind, either express or implied.
 * see the apache license version 2.0 for the specific language governing permissions and
 * limitations there under.
 */
package com.snowplowanalytics.snowplow.storage.spark
package bad

import java.io.File

import org.specs2.mutable.Specification

object SchemaValidationFailedSpec {
  import ShredJobSpec._
  val lines = Lines(
    """snowplowweb	web	2014-06-01 14:04:11.639	2014-05-29 18:16:35.000	2014-05-29 18:16:35.967	unstruct	2b1b25a4-c0df-4859-8201-cf21492ad61b	114221	clojure	js-2.0.0-M2	clj-0.6.0-tom-0.0.4	hadoop-0.5.0-common-0.4.0		68.42.204.218	1242058182	58df65c46e1ac937	11	437ad25b-2006-455e-b5d8-d664b74df8f3	US	MI	Holland	49423	42.742294	-86.0661						http://snowplowanalytics.com/blog/		https://www.google.com/	http	snowplowanalytics.com	80	/blog/			https	www.google.com	80	/			search	Google													{"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0","data":{"schema":"iglu:com.snowplowanalytics.snowplow/link_click/jsonschema/1-0-0","data":{"elementClasses":["next"]}}}																			Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.114 Safari/537.36	Chrome	Chrome		Browser	WEBKIT	en-US	1	1	1	0	1	0	0	0	1	1	24	1241	806	Mac OS	Mac OS	Apple Inc.	America/New_York	Computer	0	1440	900	UTF-8	1226	3874"""
  )
  val expected = (line: String) =>
    """|{"line":"%s","errors":[
      |{"level":"error","schema":{"loadingURI":"#","pointer":""},"instance":{"pointer":""},"domain":"validation","keyword":"required","message":"object has missing required properties ([\"targetUrl\"])","required":["targetUrl"],"missing":["targetUrl"]}
      |]}""".stripMargin.format(line.replaceAll("\"", "\\\\\"")).replaceAll("[\n\r]","").replaceAll("[\t]","\\\\t")
}

class SchemaValidationFailedSpec extends Specification with ShredJobSpec {
  import ShredJobSpec._
  override def appName = "schema-validation-failed"
  sequential
  "A job which processes input lines with invalid Snowplow enriched events" should {
    runShredJob(SchemaValidationFailedSpec.lines)

    "write a bad row JSON with input line and error message for each input line" in {
      val Some((jsons, _)) = readPartFile(dirs.badRows, "")
      jsons.map(removeTstamp) must_==
        SchemaValidationFailedSpec.lines.lines.map(SchemaValidationFailedSpec.expected)
    }

    "not write any atomic-events" in {
      new File(dirs.output, "atomic-events") must beEmptyDir
    }
    "not write any jsons" in {
      new File(dirs.output, "shredded-types") must beEmptyDir
    }
  }
}
