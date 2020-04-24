/*
 * Copyright (c) 2016-2020 Snowplow Analytics Ltd. All rights reserved.
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

object CljTomcatOlarkEventSpec {
  import EnrichJobSpec._
  val lines = Lines(
    "2014-10-09  16:28:31    -   13  255.255.255.255   POST    255.255.255.255   /com.olark/v1   404 -  -    aid=email&cv=clj-0.6.0-tom-0.0.4&nuid=-   -   -   -   application%2Fx-www-form-urlencoded   ZGF0YT0lN0IlMjJraW5kJTIyJTNBKyUyMkNvbnZlcnNhdGlvbiUyMiUyQyslMjJ0YWdzJTIyJTNBKyU1QiUyMnRlc3RfZXhhbXBsZSUyMiU1RCUyQyslMjJpdGVtcyUyMiUzQSslNUIlN0IlMjJib2R5JTIyJTNBKyUyMkhpK2Zyb20rYW4rb3BlcmF0b3IlMjIlMkMrJTIydGltZXN0YW1wJTIyJTNBKyUyMjE0NzM3NzQ4MTkuMjYzMDgzJTIyJTJDKyUyMmtpbmQlMjIlM0ErJTIyTWVzc2FnZVRvVmlzaXRvciUyMiUyQyslMjJuaWNrbmFtZSUyMiUzQSslMjJPbGFyaytvcGVyYXRvciUyMiUyQyslMjJvcGVyYXRvcklkJTIyJTNBKyUyMjY0NzU2MyUyMiU3RCUyQyslN0IlMjJib2R5JTIyJTNBKyUyMkhpK2Zyb20rYSt2aXNpdG9yJTIyJTJDKyUyMnRpbWVzdGFtcCUyMiUzQSslMjIxNDczNzc0ODIxLjQxMTE1NCUyMiUyQyslMjJraW5kJTIyJTNBKyUyMk1lc3NhZ2VUb09wZXJhdG9yJTIyJTJDKyUyMm5pY2tuYW1lJTIyJTNBKyUyMlJldHVybmluZytWaXNpdG9yKyU3QytVU0ErJTI4U2FuK0ZyYW5jaXNjbyUyQytDQSUyOSslMjM3NjE3JTIyJTJDKyUyMnZpc2l0b3Jfbmlja25hbWUlMjIlM0ErJTIyT2xhcmsrVmlzaXRvciUyMiU3RCU1RCUyQyslMjJvcGVyYXRvcnMlMjIlM0ErJTdCJTIyNjQ3NTYzJTIyJTNBKyU3QiUyMnVzZXJuYW1lJTIyJTNBKyUyMnlhbGklMjIlMkMrJTIyZW1haWxBZGRyZXNzJTIyJTNBKyUyMnlhbGklNDBzbm93cGxvd2FuYWx5dGljcy5jb20lMjIlMkMrJTIya2luZCUyMiUzQSslMjJPcGVyYXRvciUyMiUyQyslMjJuaWNrbmFtZSUyMiUzQSslMjJZYWxpJTIyJTJDKyUyMmlkJTIyJTNBKyUyMjY0NzU2MyUyMiU3RCU3RCUyQyslMjJ2aXNpdG9yJTIyJTNBKyU3QiUyMmNpdHklMjIlM0ErJTIyU2FuK0ZyYW5jaXNjbyUyMiUyQyslMjJraW5kJTIyJTNBKyUyMlZpc2l0b3IlMjIlMkMrJTIyb3JnYW5pemF0aW9uJTIyJTNBKyUyMlZpc2l0b3IrT3JnYW5pemF0aW9uJTIyJTJDKyUyMmNvbnZlcnNhdGlvbkJlZ2luUGFnZSUyMiUzQSslMjJodHRwJTNBJTJGJTJGd3d3Lm9sYXJrLmNvbSUyMiUyQyslMjJjb3VudHJ5Q29kZSUyMiUzQSslMjJVUyUyMiUyQyslMjJyZWZlcnJlciUyMiUzQSslMjJodHRwJTNBJTJGJTJGd3d3Lm9sYXJrLmNvbSUyMiUyQyslMjJpcCUyMiUzQSslMjIxMjcuMC4wLjElMjIlMkMrJTIycmVnaW9uJTIyJTNBKyUyMkNBJTIyJTJDKyUyMmNoYXRfZmVlZGJhY2slMjIlM0ErJTdCJTIyb3ZlcmFsbF9jaGF0JTIyJTNBKzQlMkMrJTIycmVzcG9uc2l2ZW5lc3MlMjIlM0ErNSUyQyslMjJmcmllbmRsaW5lc3MlMjIlM0ErNSUyQyslMjJrbm93bGVkZ2UlMjIlM0ErNCU3RCUyQyslMjJvcGVyYXRpbmdTeXN0ZW0lMjIlM0ErJTIyV2luZG93cyUyMiUyQyslMjJlbWFpbEFkZHJlc3MlMjIlM0ErJTIyc3VwcG9ydCUyQmludGVncmF0aW9udGVzdCU0MG9sYXJrLmNvbSUyMiUyQyslMjJjb3VudHJ5JTIyJTNBKyUyMlVuaXRlZCtTdGF0ZXMlMjIlMkMrJTIycGhvbmVOdW1iZXIlMjIlM0ErJTIyNTU1NTU1NTU1NSUyMiUyQyslMjJmdWxsTmFtZSUyMiUzQSslMjJPbGFyayUyMiUyQyslMjJpZCUyMiUzQSslMjJOT1RBUkVBTFZJU0lUT1JJRFM1TEdsNlFVcksyT2FQUCUyMiUyQyslMjJicm93c2VyJTIyJTNBKyUyMkludGVybmV0K0V4cGxvcmVyKzExJTIyJTdEJTJDKyUyMmlkJTIyJTNBKyUyMk5PVEFSRUFMVFJBTlNDUklQVDVMR2NiVlRhM2hLQlJCJTIyJTJDKyUyMm1hbnVhbGx5U3VibWl0dGVkJTIyJTNBK2ZhbHNlJTdE"
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
    "com.olark-v1",
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
    """{"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0","data":{"schema":"iglu:com.olark/transcript/jsonschema/1-0-0","data":{"kind":"Conversation","tags":["test_example"],"items":[{"body":"Hi from an operator","timestamp":"2016-09-13T13:53:39.263Z","kind":"MessageToVisitor","nickname":"Olark operator","operatorId":"647563"},{"body":"Hi from a visitor","timestamp":"2016-09-13T13:53:41.411Z","kind":"MessageToOperator","nickname":"Returning Visitor | USA (San Francisco, CA) #7617","visitorNickname":"Olark Visitor"}],"operators":{"647563":{"username":"yali","emailAddress":"yali@snowplowanalytics.com","kind":"Operator","nickname":"Yali","id":"647563"}},"visitor":{"city":"San Francisco","kind":"Visitor","organization":"Visitor Organization","conversationBeginPage":"http://www.olark.com","countryCode":"US","referrer":"http://www.olark.com","ip":"127.0.0.1","region":"CA","chatFeedback":{"overallChat":4,"responsiveness":5,"friendliness":5,"knowledge":4},"operatingSystem":"Windows","emailAddress":"support+integrationtest@olark.com","country":"United States","phoneNumber":"5555555555","fullName":"Olark","id":"NOTAREALVISITORIDS5LGl6QUrK2OaPP","browser":"Internet Explorer 11"},"id":"NOTAREALTRANSCRIPT5LGcbVTa3hKBRB","manuallySubmitted":false}}}""",
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

class CljTomcatOlarkEventSpec extends Specification with EnrichJobSpec {
  import EnrichJobSpec._
  override def appName = "clj-tomcat-olark-event"
  sequential
  "A job which processes a Clojure-Tomcat file containing a Olark POST raw event representing 1 valid completed call" should {
    runEnrichJob(CljTomcatOlarkEventSpec.lines, "clj-tomcat", "2", true, List("geo"))
    "correctly output 1 completed call" in {
      val Some(goods) = readPartFile(dirs.output)
      goods.size must_== 1
      val actual = goods.head.split("\t").map(s => if (s.isEmpty()) null else s)
      for (idx <- CljTomcatOlarkEventSpec.expected.indices) {
        Try(parse(CljTomcatOlarkEventSpec.expected(idx))) match {
          case Success(parsedJSON) => parse(actual(idx)) must beEqualTo(parsedJSON)
          case Failure(msg) =>
            actual(idx) must BeFieldEqualTo(CljTomcatOlarkEventSpec.expected(idx), idx)
        }
      }
    }

    "not write any bad rows" in {
      dirs.badRows must beEmptyDir
    }
  }
}
