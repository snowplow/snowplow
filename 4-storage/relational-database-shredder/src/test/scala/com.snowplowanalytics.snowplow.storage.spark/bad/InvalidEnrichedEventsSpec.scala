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

object InvalidEnrichedEventsSpec {
  import ShredJobSpec._
  val lines = Lines(
    """snowplowweb	web	2014-06-01 18:04:11.639	29th May 2013 18:04:12	2014-05-29 18:04:11.639	page_view	not-a-uuid	836413	clojure	js-2.0.0-M2	clj-0.6.0-tom-0.0.4	hadoop-0.5.0-common-0.4.0		216.207.42.134	3499345421	3b1d1a375044eede	3	2bad2a4e-aae4-4bea-8acd-399e7fe0366a	US	CA	South San Francisco		37.654694	-122.4077						http://snowplowanalytics.com/blog/2013/02/08/writing-hive-udfs-and-serdes/	Writing Hive UDFs - a tutorial		http	snowplowanalytics.com	80	/blog/2013/02/08/writing-hive-udfs-and-serdes/																	{"schema":"iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-0","data":[{"schema":"iglu:com.snowplowanalytics.website/fake_context/jsonschema/1-0-0","data":{"author":"Alex Dean","topics":["hive","udf","serde","java","hadoop"],"subCategory":"inside the plow","category":"blog","whenPublished":"2013-02-08"}}]}																									Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.75.14 (KHTML, like Gecko) Version/7.0.3 Safari/537.75.14	Safari	Safari		Browser	WEBKIT	en-us	0	0	0	0	0	0	0	0	0	1	24	1440	1845	Mac OS	Mac OS	Apple Inc.	America/Los_Angeles	Computer	0					"""
  )
  val expected = (line: String) =>
    """{"line":"%s","errors":[{"level":"error","message":"Field [event_id]: [not-a-uuid] is not a valid UUID"},{"level":"error","message":"Field [collector_tstamp]: [29th May 2013 18:04:12] is not in the expected Redshift/Postgres timestamp format"}]}"""
      .format(line.replaceAll("\"", "\\\\\"")).replaceAll("[\n\r]","").replaceAll("[\t]","\\\\t")
}

class InvalidEnrichedEventsSpec extends Specification with ShredJobSpec {
  import ShredJobSpec._
  override def appName = "invalid-enriched-events"
  sequential
  "A job which processes input lines with invalid Snowplow enriched events" should {
    runShredJob(InvalidEnrichedEventsSpec.lines)

    "write a bad row JSON with input line and error message for each input line" in {
      val Some((jsons, _)) = readPartFile(dirs.badRows, "")
      jsons.map(removeTstamp) must_==
        InvalidEnrichedEventsSpec.lines.lines.map(InvalidEnrichedEventsSpec.expected)
    }

    "not write any atomic-events" in {
      new File(dirs.output, "atomic-events") must beEmptyDir
    }
    "not write any jsons" in {
      new File(dirs.output, "shredded-types") must beEmptyDir
    }
  }
}
