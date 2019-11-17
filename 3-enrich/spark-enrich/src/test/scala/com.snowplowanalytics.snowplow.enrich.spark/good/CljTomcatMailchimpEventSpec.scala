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

object CljTomcatMailchimpEventSpec {
  import EnrichJobSpec._
  val lines = Lines(
    "2014-10-09  16:28:31    -   13  255.255.255.255   POST    255.255.255.255   /com.mailchimp/v1   404 -  -    aid=email&cv=clj-0.6.0-tom-0.0.4&nuid=-   -   -   -   application%2Fx-www-form-urlencoded   dHlwZT1zdWJzY3JpYmUmZmlyZWRfYXQ9MjAxNC0xMS0wNCswOSUzQTQyJTNBMzEmZGF0YSU1QmlkJTVEPWU3Yzc3ZDM4NTImZGF0YSU1QmVtYWlsJTVEPWFnZW50c21pdGglNDBzbm93cGxvd3Rlc3QuY29tJmRhdGElNUJlbWFpbF90eXBlJTVEPWh0bWwmZGF0YSU1QmlwX29wdCU1RD04Mi4yMjUuMTY5LjIyMCZkYXRhJTVCd2ViX2lkJTVEPTIxMDgzMzgyNSZkYXRhJTVCbWVyZ2VzJTVEJTVCRU1BSUwlNUQ9YWdlbnRzbWl0aCU0MHNub3dwbG93dGVzdC5jb20mZGF0YSU1Qm1lcmdlcyU1RCU1QkZOQU1FJTVEPUFnZW50JmRhdGElNUJtZXJnZXMlNUQlNUJMTkFNRSU1RD1TbWl0aCZkYXRhJTVCbGlzdF9pZCU1RD1mMTI0M2EzYjEy"
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
    "com.mailchimp-v1",
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
    """{"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0","data":{"schema":"iglu:com.mailchimp/subscribe/jsonschema/1-0-0","data":{"data":{"merges":{"EMAIL":"agentsmith@snowplowtest.com","FNAME":"Agent","LNAME":"Smith"},"web_id":"210833825","id":"e7c77d3852","email_type":"html","list_id":"f1243a3b12","email":"90e0d037faf42119f4ffac694a26d892f1f3d598","ip_opt":"f7fe27cbe23fdc69e682b9306b383a22706ff778"},"type":"subscribe","fired_at":"2014-11-04T09:42:31.000Z"}}}""",
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

class CljTomcatMailchimpEventSpec extends Specification with EnrichJobSpec {
  import EnrichJobSpec._
  override def appName = "clj-tomcat-mailchimp-event"
  sequential
  "A job which processes a Clojure-Tomcat file containing a POST raw event representing 1 valid " +
    "completed call" should {
    runEnrichJob(CljTomcatMailchimpEventSpec.lines, "clj-tomcat", "2", true, List("geo"))

    "correctly output 1 completed call" in {
      val Some(goods) = readPartFile(dirs.output)
      goods.size must_== 1
      val actual = goods.head.split("\t").map(s => if (s.isEmpty()) null else s)
      for (idx <- CljTomcatMailchimpEventSpec.expected.indices) {
        actual(idx) must BeFieldEqualTo(CljTomcatMailchimpEventSpec.expected(idx), idx)
      }
    }

    "not write any bad rows" in {
      dirs.badRows must beEmptyDir
    }
  }
}
