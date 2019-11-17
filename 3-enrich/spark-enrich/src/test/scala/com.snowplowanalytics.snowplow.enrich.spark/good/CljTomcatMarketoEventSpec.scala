/*
 * Copyright (c) 2012-2019 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at
 * http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and
 * limitations there under.
 */
package com.snowplowanalytics.snowplow.enrich.spark
package good

import org.specs2.mutable.Specification

object CljTomcatMarketoEventSpec {
  import EnrichJobSpec._
  val lines = Lines(
    "2014-10-09  16:28:31    -   13  255.255.255.255   POST    255.255.255.255   /com.marketo/v1   404 -  -    aid=email&cv=clj-0.6.0-tom-0.0.4&nuid=-   -   -   -   application%2Fjson   eyJuYW1lIjogIndlYmhvb2sgZm9yIEEiLCAic3RlcCI6IDYsICJjYW1wYWlnbiI6IHsiaWQiOiAxNjAsICJuYW1lIjogImF2ZW5nZXJzIGFzc2VtYmxlIn0sICJsZWFkIjogeyJhY3F1aXNpdGlvbl9kYXRlIjogIjIwMTAtMTEtMTEgMTE6MTE6MTEiLCAiYmxhY2tfbGlzdGVkIjogZmFsc2UsICJmaXJzdF9uYW1lIjogInRoZSBodWxrIiwgInVwZGF0ZWRfYXQiOiAiMjAxOC0wNi0xNiAxMToyMzo1OCIsICJjcmVhdGVkX2F0IjogIjIwMTgtMDYtMTYgMTE6MjM6NTgiLCAibGFzdF9pbnRlcmVzdGluZ19tb21lbnRfZGF0ZSI6ICIyMDE4LTA5LTI2IDIwOjI2OjQwIn0sICJjb21wYW55IjogeyJuYW1lIjogImlyb24gbWFuIiwgIm5vdGVzIjogInRoZSBzb21ldGhpbmcgZG9nIGxlYXB0IG92ZXIgdGhlIGxhenkgZm94In0sICJjYW1wYWlnbiI6IHsiaWQiOiA5ODcsICJuYW1lIjogInRyaWdnZXJlZCBldmVudCJ9LCAiZGF0ZXRpbWUiOiAiMjAxOC0wMy0wNyAxNDoyODoxNiJ9Cg=="
  )
  val expected = List(
    "email",
    "srv",
    etlTimestamp,
    "2014-10-09 16:28:31.000",
    null,
    "unstruct",
    null, // We can't predict the event_id
    null,
    null, // No tracker namespace
    "com.marketo-v1",
    "clj-0.6.0-tom-0.0.4",
    etlVersion,
    null, // No user_id set
    "79398dd7e78a8998b6e58e380e7168d8766f1644",
    null,
    null,
    null,
    "-", // TODO: fix this, https://github.com/snowplow/snowplow/issues/1133
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
    null, // No custom contexts
    null, // Structured event fields empty
    null, //
    null, //
    null, //
    null, //
    """{"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0","data":{"schema":"iglu:com.marketo/event/jsonschema/2-0-0","data":{"lead":{"first_name":"the hulk","acquisition_date":"2010-11-11T11:11:11.000Z","black_listed":false,"last_interesting_moment_date":"2018-09-26T20:26:40.000Z","created_at":"2018-06-16T11:23:58.000Z","updated_at":"2018-06-16T11:23:58.000Z"},"name":"webhook for A","step":6,"campaign":{"id":987,"name":"triggered event"},"datetime":"2018-03-07T14:28:16.000Z","company":{"name":"iron man","notes":"the something dog leapt over the lazy fox"}}}}""",
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
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null
  )
}

class CljTomcatMarketoEventSpec extends Specification with EnrichJobSpec {
  import EnrichJobSpec._
  override def appName = "clj-tomcat-marketo-event"
  sequential
  "A job which processes a Clojure-Tomcat file containing a POST raw event representing 1 valid " +
    "marketo event" should {
    runEnrichJob(CljTomcatMarketoEventSpec.lines, "clj-tomcat", "2", true, List("geo"))

    "correctly output 1 marketo event" in {
      val Some(goods) = readPartFile(dirs.output)
      goods.size must_== 1
      val actual = goods.head.split("\t").map(s => if (s.isEmpty()) null else s)
      for (idx <- CljTomcatMarketoEventSpec.expected.indices) {
        actual(idx) must BeFieldEqualTo(CljTomcatMarketoEventSpec.expected(idx), idx)
      }
    }

    "not write any bad rows" in {
      dirs.badRows must beEmptyDir
    }
  }
}
