/*
 * Copyright (c) 2016-2018 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.enrich.spark
package good

// Specs2
import org.specs2.mutable.Specification

import scala.util.{Failure, Success, Try}

//json4s
import org.json4s._
import org.json4s.jackson.JsonMethods._

/**
 * Holds the input and expected data
 * for the test.
 */
object CljTomcatUnbounceEventSpec {
  import EnrichJobSpec._
  val lines = Lines(
    "2017-11-15 12:12:36  - - 52.91.101.141 POST  50.19.99.184  /com.unbounce/v1  200 - Apache-HttpClient%2F4.5.1+%28Java%2F1.8.0_101%29  &cv=clj-1.1.0-tom-0.2.0&nuid=9965b812-9030-42de-8c18-41213abcf73b - - - application%2Fx-www-form-urlencoded cGFnZV91cmw9aHR0cCUzQSUyRiUyRnVuYm91bmNlcGFnZXMuY29tJTJGd2F5ZmFyaW5nLTE0NyUyRiZwYWdlX25hbWU9V2F5ZmFyaW5nJnBhZ2VfaWQ9NzY0ODE3N2QtNzMyMy00MzMwLWI0ZjktOTk1MWE1MjEzOGI2JnZhcmlhbnQ9YSZkYXRhLmpzb249JTdCJTIydXNlcmZpZWxkMSUyMiUzQSU1QiUyMmFzZGZhc2RmYWQlMjIlNUQlMkMlMjJpcF9hZGRyZXNzJTIyJTNBJTVCJTIyODUuNzMuMzkuMTYzJTIyJTVEJTJDJTIycGFnZV91dWlkJTIyJTNBJTVCJTIyNzY0ODE3N2QtNzMyMy00MzMwLWI0ZjktOTk1MWE1MjEzOGI2JTIyJTVEJTJDJTIydmFyaWFudCUyMiUzQSU1QiUyMmElMjIlNUQlMkMlMjJ0aW1lX3N1Ym1pdHRlZCUyMiUzQSU1QiUyMjEyJTNBMTIrUE0rVVRDJTIyJTVEJTJDJTIyZGF0ZV9zdWJtaXR0ZWQlMjIlM0ElNUIlMjIyMDE3LTExLTE1JTIyJTVEJTJDJTIycGFnZV91cmwlMjIlM0ElNUIlMjJodHRwJTNBJTJGJTJGdW5ib3VuY2VwYWdlcy5jb20lMkZ3YXlmYXJpbmctMTQ3JTJGJTIyJTVEJTJDJTIycGFnZV9uYW1lJTIyJTNBJTVCJTIyV2F5ZmFyaW5nJTIyJTVEJTdEJmRhdGEueG1sPSUzQyUzRnhtbCt2ZXJzaW9uJTNEJTIyMS4wJTIyK2VuY29kaW5nJTNEJTIyVVRGLTglMjIlM0YlM0UlM0Nmb3JtX2RhdGElM0UlM0N1c2VyZmllbGQxJTNFYXNkZmFzZGZhZCUzQyUyRnVzZXJmaWVsZDElM0UlM0NpcF9hZGRyZXNzJTNFODUuNzMuMzkuMTYzJTNDJTJGaXBfYWRkcmVzcyUzRSUzQ3BhZ2VfdXVpZCUzRTc2NDgxNzdkLTczMjMtNDMzMC1iNGY5LTk5NTFhNTIxMzhiNiUzQyUyRnBhZ2VfdXVpZCUzRSUzQ3ZhcmlhbnQlM0VhJTNDJTJGdmFyaWFudCUzRSUzQ3RpbWVfc3VibWl0dGVkJTNFMTIlM0ExMitQTStVVEMlM0MlMkZ0aW1lX3N1Ym1pdHRlZCUzRSUzQ2RhdGVfc3VibWl0dGVkJTNFMjAxNy0xMS0xNSUzQyUyRmRhdGVfc3VibWl0dGVkJTNFJTNDcGFnZV91cmwlM0VodHRwJTNBJTJGJTJGdW5ib3VuY2VwYWdlcy5jb20lMkZ3YXlmYXJpbmctMTQ3JTJGJTNDJTJGcGFnZV91cmwlM0UlM0NwYWdlX25hbWUlM0VXYXlmYXJpbmclM0MlMkZwYWdlX25hbWUlM0UlM0MlMkZmb3JtX2RhdGElM0U"
    )

  val expected = List(
    null,
    "srv",
    etlTimestamp,
    "2017-11-15 12:12:36.000",
    null,
    "unstruct",
    null, // We can't predict the event_id
    null,
    null, // No tracker namespace
    "com.unbounce-v1",
    "clj-1.1.0-tom-0.2.0",
    etlVersion,
    null, // No user_id set
    "52.91.x.x",
    null,
    null,
    null,
    "9965b812-9030-42de-8c18-41213abcf73b",
    null, // No geo-location for this IP address
    null,
    null,
    null,
    null,
    null,
    null,
    null, // No additional MaxMind databases used
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null, // Marketing campaign fields empty
    null, //
    null, //
    null, //
    null, //
    null,
    null, // Structured event fields empty
    null, //
    null, //
    null, //
    null, //
    """{"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0","data":{"schema":"iglu:com.unbounce/form_post/jsonschema/1-0-0","data":{"data.json":{"userfield1":["asdfasdfad"],"ipAddress":["85.73.39.163"],"pageUuid":["7648177d-7323-4330-b4f9-9951a52138b6"],"variant":["a"],"timeSubmitted":["12:12 PM UTC"],"dateSubmitted":["2017-11-15"],"pageUrl":["http://unbouncepages.com/wayfaring-147/"],"pageName":["Wayfaring"]},"variant":"a","pageId":"7648177d-7323-4330-b4f9-9951a52138b6","pageName":"Wayfaring","pageUrl":"http://unbouncepages.com/wayfaring-147/"}}}""",
    null, // Transaction fields empty
    null, //
    null, //
    null, //
    null, //
    null, //
    null, //
    null, //
    null, // Transaction item fields empty
    null, //
    null, //
    null, //
    null, //
    null, //
    null, // Page ping fields empty
    null, //
    null, //
    null, //
    "Apache-HttpClient/4.5.1 (Java/1.8.0_101)",
    "Downloading Tool",
    "Downloading Tool",
    null,
    "Downloading tool",
    "OTHER",
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    "Unknown",
    "Unknown",
    "Other",
    null,
    "Unknown",
    "0",
    null,
    null,
    null,
    null,
    null
    )
}

class CljTomcatUnbounceEventSpec extends Specification with EnrichJobSpec {
  import EnrichJobSpec._
  override def appName = "clj-tomcat-unbounce-event"
  sequential
  "A job which processes a Clojure-Tomcat file containing a Unbounce POST raw event representing 1 valid completed call" should {
    runEnrichJob(CljTomcatUnbounceEventSpec.lines, "clj-tomcat", "2", true, List("geo"))
    "correctly output 1 completed call" in {
      val Some(goods) = readPartFile(dirs.output)
      goods.size must_== 1
      val actual = goods.head.split("\t").map(s => if (s.isEmpty()) null else s)
      for (idx <- CljTomcatUnbounceEventSpec.expected.indices) {
        Try(parse(CljTomcatUnbounceEventSpec.expected(idx))) match {
          case Success(parsedJSON) => parse(actual(idx)) must beEqualTo(parsedJSON)
          case Failure(msg) => actual(idx) must BeFieldEqualTo(CljTomcatUnbounceEventSpec.expected(idx), idx)
        }
      }
    }

    "not write any bad rows" in {
      dirs.badRows must beEmptyDir
    }
  }
}
