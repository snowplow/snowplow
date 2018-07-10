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

/**
 * Holds the input and expected data
 * for the test.
 */
object CljTomcatStatusGatorEventSpec {
  import EnrichJobSpec._
  val lines = Lines(
    "2017-11-11 15:37:08  - - 54.205.110.234  POST  54.205.110.234  /com.statusgator/v1 200 - Ruby  &cv=clj-1.1.0-tom-0.2.0&nuid=61d4eae4-f00f-4a87-8d53-8a396a1a77a6 - - - application%2Fx-www-form-urlencoded c2VydmljZV9uYW1lPUFtYXpvbitXZWIrU2VydmljZXMmZmF2aWNvbl91cmw9aHR0cHMlM0ElMkYlMkZkd3hqZDljZDZyd25vLmNsb3VkZnJvbnQubmV0JTJGZmF2aWNvbnMlMkZhbWF6b24td2ViLXNlcnZpY2VzLmljbyZzdGF0dXNfcGFnZV91cmw9aHR0cCUzQSUyRiUyRnN0YXR1cy5hd3MuYW1hem9uLmNvbSUyRiZob21lX3BhZ2VfdXJsPWh0dHAlM0ElMkYlMkZhd3MuYW1hem9uLmNvbSUyRiZjdXJyZW50X3N0YXR1cz13YXJuJmxhc3Rfc3RhdHVzPXVwJm9jY3VycmVkX2F0PTIwMTctMTEtMTFUMTUlM0EzNiUzQTE4JTJCMDAlM0EwMA"
  )

  val expected = List(
    null,
    "srv",
    etlTimestamp,
    "2017-11-11 15:37:08.000",
    null,
    "unstruct",
    null, // We can't predict the event_id
    null,
    null, // No tracker namespace
    "com.statusgator-v1",
    "clj-1.1.0-tom-0.2.0",
    etlVersion,
    null, // No user_id set
    "b861eeeeb806fd4120237a502ce257b162e11edb",
    null,
    null,
    null,
    "61d4eae4-f00f-4a87-8d53-8a396a1a77a6",
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
    """{"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0","data":{"schema":"iglu:com.statusgator/status_change/jsonschema/1-0-0","data":{"lastStatus":"up","statusPageUrl":"http://status.aws.amazon.com/","serviceName":"Amazon Web Services","faviconUrl":"https://dwxjd9cd6rwno.cloudfront.net/favicons/amazon-web-services.ico","occurredAt":"2017-11-11T15:36:18+00:00","homePageUrl":"http://aws.amazon.com/","currentStatus":"warn"}}}""",
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
    "Ruby",
    "Unknown",
    "Unknown",
    null,
    "unknown",
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

class CljTomcatStatusGatorEventSpec extends Specification with EnrichJobSpec {
  import EnrichJobSpec._
  override def appName = "clj-tomcat-statusgator-event"
  sequential
  "A job which processes a Clojure-Tomcat file containing a StatusGator POST raw event representing 1 valid completed call" should {

    runEnrichJob(CljTomcatStatusGatorEventSpec.lines, "clj-tomcat", "2", true, List("geo"))

    "correctly output 1 completed call" in {
      val Some(goods) = readPartFile(dirs.output)
      goods.size must_== 1
      val actual = goods.head.split("\t").map(s => if (s.isEmpty()) null else s)
      for (idx <- CljTomcatStatusGatorEventSpec.expected.indices) {
        actual(idx) must BeFieldEqualTo(CljTomcatStatusGatorEventSpec.expected(idx), idx)
      }
    }

    "not write any bad rows" in {
      dirs.badRows must beEmptyDir
    }
  }
}
