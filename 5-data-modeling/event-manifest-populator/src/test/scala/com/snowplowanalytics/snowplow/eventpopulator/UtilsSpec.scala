/*
 * Copyright (c) 2017 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.eventpopulator

// joda time
import org.joda.time.DateTime

// specs2
import org.specs2.Specification

class UtilsSpec extends Specification { def is = s2"""

  This is a specification for the Event Manifest Populator util functions

    filterSince should not include passed `since` item $e1
    filterSince should filter and sort dates correctly $e2
    parseInput should accept both short and precise formats $e3
    parseRunId should parse valid runId and drop invalid $e4
    lineToTriple must correctly extract eventId, etlTime and fingerprint from Enriched Event TSV $e5
                                                      """
  def e1 = {
    val runs: List[DateTime] = List(
      DateTime.parse("2016-06-30T00:20:00.000+07:00"),
      DateTime.parse("2016-06-30T04:20:00.000+07:00"),
      DateTime.parse("2016-06-30T08:20:00.000+07:00"),
      DateTime.parse("2016-06-30T12:20:00.000+07:00"),
      DateTime.parse("2016-06-30T16:20:00.000+07:00"))
    val since = DateTime.parse("2016-06-30T11:20:00.000+07:00")

    val result = Utils.filterSince(runs, since)
    result should not contain since
  }

  def e2 = {
    val runs: List[DateTime] = List(
      DateTime.parse("2016-06-30T08:20:00.000+07:00"), // 3
      DateTime.parse("2016-06-30T16:20:00.000+07:00"), // 5
      DateTime.parse("2016-06-30T12:20:00.000+07:00"), // 4
      DateTime.parse("2016-06-30T00:20:00.000+07:00"), // 1
      DateTime.parse("2016-06-30T04:20:00.000+07:00")) // 2
    val since = DateTime.parse("2016-06-30T11:20:00.000+07:00")

    val expected = List(
      DateTime.parse("2016-06-30T12:20:00.000+07:00"),
      DateTime.parse("2016-06-30T16:20:00.000+07:00"))

    val result = Utils.filterSince(runs, since)
    result must beEqualTo(expected)
  }

  def e3 = {
    val short = Utils.parseInput("2017-03-30")
    val precise = Utils.parseInput("2017-04-30-12-30-00")

    val expectedShort = DateTime.parse("2017-03-30T00:00:00.000")
    val expectedPrecise = DateTime.parse("2017-04-30T12:30:00.000")

    val shortResult = short must beEqualTo(expectedShort)
    val preciseResult = precise must beEqualTo(expectedPrecise)

    shortResult.and(preciseResult)
  }

  def e4 = {
    val validRunId = Utils.parseRunId("2017-04-30-12-30-00/")
    val validRunIdWithoutSlash = Utils.parseRunId("2017-04-30-12-30-00")
    val invalidRunId = Utils.parseRunId("2017-28-30")

    (validRunId must beSome).and(invalidRunId must beNone).and(validRunIdWithoutSlash must beSome)
  }

  def e5 = {
    val line = s"""blog	web	2016-11-27 08:46:40.000	2016-11-27 07:16:07.000	2016-11-27 07:16:07.333	page_view	1799a90f-f570-4414-b91a-b0db8f39cc2e		blogTracker	js-2.7.0-rc2	clj-1.1.0-tom-0.2.0	hadoop-1.8.0-common-0.24.0		185.124.153.x	531497290	1f9b3980-6619-4d75-a6c9-8253c76c3bfb	18	5beb1f92-d4fb-4020-905c-f659929c8ab5												http://chuwy.me/scala-blocks.html	Scala Code Blocks	http://chuwy.me/	http	chuwy.me	80	/scala-blocks.html			http	chuwy.me	80	/			internal																																	Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.98 Safari/537.36	Chrome	Chrome	54.0.2840.98	Browser	WEBKIT		1	1	0	0	0	0	0	0	0	1	24	1280	726	Mac OS X	Mac OS X	Apple Inc.	Asia/Omsk	Computer	0	1280	800	UTF-8	1280	4315												2016-11-27 07:16:07.340			{"schema":"iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-1","data":[{"schema":"iglu:com.snowplowanalytics.snowplow/ua_parser_context/jsonschema/1-0-0","data":{"useragentFamily":"Chrome","useragentMajor":"54","useragentMinor":"0","useragentPatch":"2840","useragentVersion":"Chrome 54.0.2840","osFamily":"MacOS X","osMajor":"10","osMinor":"11","osPatch":"6","osPatchMinor":null,"osVersion":"Mac OS X 10.11.6","deviceFamily":"Other"}}]}	395e4506-37a3-4074-8de2-d8c75fb17d4a	2016-11-27 07:16:06.993	com.snowplowanalytics.snowplow	page_view	jsonschema	1-0-0	bed9a39a0917874d2ff072033a6413d9	"""
    val expected = Utils.DeduplicationTriple("1799a90f-f570-4414-b91a-b0db8f39cc2e", "bed9a39a0917874d2ff072033a6413d9", "2016-11-27 08:46:40.000")

    Utils.lineToTriple(line) must beEqualTo(expected)
  }
}
