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

object CljTomcatHubspotEventSpec {
  import EnrichJobSpec._
  val lines = Lines(
    "2014-10-09  16:28:31    -   13  255.255.255.255   POST    255.255.255.255   /com.hubspot/v1   404 -  -    aid=email&cv=clj-0.6.0-tom-0.0.4&nuid=-   -   -   -   application%2Fjson   W3siZXZlbnRJZCI6MSwic3Vic2NyaXB0aW9uSWQiOjI1NDU4LCJwb3J0YWxJZCI6NDczNzgxOCwib2NjdXJyZWRBdCI6MTUzOTE0NTM5OTg0NSwic3Vic2NyaXB0aW9uVHlwZSI6ImNvbnRhY3QuY3JlYXRpb24iLCJhdHRlbXB0TnVtYmVyIjowLCJvYmplY3RJZCI6MTIzLCJjaGFuZ2VTb3VyY2UiOiJDUk0iLCJjaGFuZ2VGbGFnIjoiTkVXIiwiYXBwSWQiOjE3NzY5OH1dCg=="
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
    "com.hubspot-v1",
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
    """{"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0","data":{"schema":"iglu:com.hubspot/contact_creation/jsonschema/1-0-0","data":{"eventId":1,"subscriptionId":25458,"portalId":4737818,"occurredAt":"2018-10-10T04:23:19.845Z","attemptNumber":0,"objectId":123,"changeSource":"CRM","changeFlag":"NEW","appId":177698}}}""",
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

class CljTomcatHubspotEventSpec extends Specification with EnrichJobSpec {
  import EnrichJobSpec._
  override def appName = "clj-tomcat-hubspot-event"
  sequential
  "A job which processes a Clojure-Tomcat file containing a POST raw event representing 1 valid " +
    "hubspot event" should {
    runEnrichJob(CljTomcatHubspotEventSpec.lines, "clj-tomcat", "2", true, List("geo"))

    "correctly output 1 hubspot event" in {
      val Some(goods) = readPartFile(dirs.output)
      goods.size must_== 1
      val actual = goods.head.split("\t").map(s => if (s.isEmpty()) null else s)
      for (idx <- CljTomcatHubspotEventSpec.expected.indices) {
        actual(idx) must BeFieldEqualTo(CljTomcatHubspotEventSpec.expected(idx), idx)
      }
    }

    "not write any bad rows" in {
      dirs.badRows must beEmptyDir
    }
  }
}
