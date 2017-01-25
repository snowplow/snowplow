/*
 * Copyright (c) 2012-2017 Snowplow Analytics Ltd. All rights reserved.
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

object CljTomcatPingdomEventSpec {
  import EnrichJobSpec._
  val lines = Lines(
    "2014-10-09  16:28:31    -   13  255.255.255.255   POST    255.255.255.255   /com.pingdom/v1   404 -  -   p=srv&message=%7B%22check%22%3A%20%221421338%22%2C%20%22checkname%22%3A%20%22Webhooks_Test%22%2C%20%22host%22%3A%20%227eef51c2.ngrok.com%22%2C%20%22action%22%3A%20%22assign%22%2C%20%22incidentid%22%3A%203%2C%20%22description%22%3A%20%22down%22%7D&aid=uptime&cv=clj-0.6.0-tom-0.0.4&nuid=-   -   -   -   -   -"
  )
  val expected = List(
    "uptime",
    "srv",
    etlTimestamp,
    "2014-10-09 16:28:31.000",
    null,
    "unstruct",
    null, // We can't predict the event_id
    null,
    null, // No tracker namespace
    "com.pingdom-v1",
    "clj-0.6.0-tom-0.0.4",
    etlVersion,
    null, // No user_id set
    "255.255.x.x",
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
    """{"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0","data":{"schema":"iglu:com.pingdom/incident_assign/jsonschema/1-0-0","data":{"check":"1421338","checkname":"Webhooks_Test","host":"7eef51c2.ngrok.com","incidentid":3,"description":"down"}}}""",
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

class CljTomcatPingdomEventSpec extends Specification with EnrichJobSpec {
  import EnrichJobSpec._
  override def appName = "clj-tomcat-pingdom-event"
  sequential
  "A job which processes a Clojure-Tomcat file containing a Pingdom GET raw event representing 1 " +
  "valid completed call" should {
    runEnrichJob(CljTomcatPingdomEventSpec.lines, "clj-tomcat", "2", true, List("geo"))

    "correctly output 1 completed call" in {
      val Some(goods) = readPartFile(dirs.output)
      goods.size must_== 1
      val actual = goods.head.split("\t").map(s => if (s.isEmpty()) null else s)
      for (idx <- CljTomcatPingdomEventSpec.expected.indices) {
        actual(idx) must beFieldEqualTo(CljTomcatPingdomEventSpec.expected(idx), idx)
      }

      "not write any bad rows" in {
        dirs.badRows must beEmptyDir
      }
    }
  }
}
