/*
 * Copyright (c) 2016 Snowplow Analytics Ltd. All rights reserved.
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
    "2014-10-09  16:28:31    -   13  255.255.255.255   POST    255.255.255.255   /com.unbounce/v1   404 -  -    aid=email&cv=clj-0.6.0-tom-0.0.4&nuid=-   -   -   -   application%2Fx-www-form-urlencoded   ZGF0YS5qc29uPSU3QiUyMCUyMmZ1bGxfbmFtZSUyMiUzQSUyMCU1QiUyMlN0ZXZlJTIwSm9icyUyMiU1RCUyQyUyMCUyMmVtYWlsX2FkZHJlc3MlMjIlM0ElMjAlNUIlMjJzdGV2ZSU0MGFwcGxlLmNvbSUyMiU1RCUyQyUyMCUyMmVhcm5pbmdzJTIyJTNBJTIwJTVCJTIyTW9yZSUyMHRoYW4lMjB5b3UlMjBjYW4lMjBwb3NzaWJseSUyMGltYWdpbmUlMjIlNUQlMkMlMjAlMjJ3aGF0X3dvdWxkX3lvdV9saWtlX291cl9wcm9kdWN0X3RvX2RvJTNGJTIyJTNBJTIwJTVCJTIyRGljZSUyMiUyQyUyMCUyMkp1bGllbm5lJTIwRnJpZXMlMjIlNUQlMkMlMjAlMjJob3dfZGlkX3lvdV9oZWFyX2Fib3V0X3VzJTNGJTIyJTNBJTIwJTVCJTIyQ2FuJTI3dCUyMFJlbWVtYmVyJTIyJTVEJTJDJTIwJTIyaXBfYWRkcmVzcyUyMiUzQSUyMCUyMjIzOC4xNy4xNTkuNCUyMiU3RCZkYXRhLnhtbD0lM0MlM0Z4bWwlMjB2ZXJzaW9uJTNEMS4wJTNGJTNFJTNDZm9ybV9kYXRhJTNFJTIwJTNDZnVsbF9uYW1lJTNFU3RldmUlMjBKb2JzJTNDJTJGZnVsbF9uYW1lJTNFJTIwJTNDZW1haWxfYWRkcmVzcyUzRXN0ZXZlJTQwYXBwbGUuY29tJTNDJTJGZW1haWxfYWRkcmVzcyUzRSUyMCUzQ2Vhcm5pbmdzJTNFTW9yZSUyMHRoYW4lMjB5b3UlMjBjYW4lMjBwb3NzaWJseSUyMGltYWdpbmUlM0MlMkZlYXJuaW5ncyUzRSUyMCUzQ3doYXRfd291bGRfeW91X2xpa2Vfb3VyX3Byb2R1Y3RfdG9fZG8lM0YlM0UlMjAlMjAlM0NlbnRyeSUzRURpY2UlM0MlMkZlbnRyeSUzRSUyMCUyMCUzQ2VudHJ5JTNFSnVsaWVubmUlMjBGcmllcyUzQyUyRmVudHJ5JTNFJTIwJTNDJTJGd2hhdF93b3VsZF95b3VfbGlrZV9vdXJfcHJvZHVjdF90b19kbyUzRiUzRSUyMCUzQ2hvd19kaWRfeW91X2hlYXJfYWJvdXRfdXMlM0YlM0VDYW4lMjd0JTIwUmVtZW1iZXIlM0MlMkZob3dfZGlkX3lvdV9oZWFyX2Fib3V0X3VzJTNGJTNFJTIwJTNDaXBfYWRkcmVzcyUzRTIzOC4xNy4xNTkuNCUzQyUyRmlwX2FkZHJlc3MlM0UlM0MlMkZmb3JtX2RhdGElM0UmcGFnZV9uYW1lPU15JTIwR3VhcmFudGVlZCUyMHRvJTIwQ29udmVydCUyMExhbmRpbmclMjBQYWdlJnBhZ2VfdXJsPWh0dHAlM0ElMkYlMkZ3d3cuZXhhbXBsZS5jb20mdmFyaWFudD1hJnBhZ2VfaWQ9YTI4MzhkOTgtNGNmNC0xMWRmLWEzZmQtMDAxNjNlMzcyZDU4"
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
    "com.unbounce-v1",
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
    null,
    null, // Structured event fields empty
    null, //
    null, //
    null, //
    null, //
    """{"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0","data":{"schema":"iglu:com.unbounce/event_context/jsonschema/1-0-0","data":{"variant":"a","data.json":{"full_name":["Steve Jobs"],"email_address":["steve@apple.com"],"earnings":["More than you can possibly imagine"],"what_would_you_like_our_product_to_do?":["Dice","Julienne Fries"],"how_did_you_hear_about_us?":["Can't Remember"],"ip_address":"238.17.159.4"},"page_id":"a2838d98-4cf4-11df-a3fd-00163e372d58","page_name":"My Guaranteed to Convert Landing Page","data.xml":"<?xml version=1.0?><form_data> <full_name>Steve Jobs</full_name> <email_address>steve@apple.com</email_address> <earnings>More than you can possibly imagine</earnings> <what_would_you_like_our_product_to_do?>  <entry>Dice</entry>  <entry>Julienne Fries</entry> </what_would_you_like_our_product_to_do?> <how_did_you_hear_about_us?>Can't Remember</how_did_you_hear_about_us?> <ip_address>238.17.159.4</ip_address></form_data>","page_url":"http://www.example.com"}}}""",
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
