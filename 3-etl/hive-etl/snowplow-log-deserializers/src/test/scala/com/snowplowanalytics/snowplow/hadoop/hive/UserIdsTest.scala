/*
 * Copyright (c) 2012-2013 SnowPlow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.hadoop.hive

// Specs2
import org.specs2.mutable.Specification

// SnowPlow Utils
import com.snowplowanalytics.util.Tap._

// Deserializer
import test.{SnowPlowDeserializer, SnowPlowEvent}

class UserIdsTest extends Specification {

  // Toggle if tests are failing and you want to inspect the struct contents
  implicit val _DEBUG = false

  val row = "2012-12-03 04:49:53  - 37  127.0.0.1 GET localhost /i  200 http://yalisassoon.github.com/cl-collector-tests/async.html Mozilla%2F5.0+%28Windows+NT+6.1%3B+WOW64%3B+rv%3A16.0%29+Gecko%2F20100101+Firefox%2F16.0  ?ev_ca=Mixes&ev_ac=Play&ev_la=MRC%2Ffabric-0503-mix&ev_va=0.0&p=web&tid=755049&duid=915bc1dd0a4c5ba5&fp=2196241488&vid=3&uid=alex%40snowplowanalytics.com&tv=js-0.8.0&lang=en-GB&refr=http%3A%2F%2Fyalisassoon.github.com%2F&f_pdf=1&f_qt=1&f_realp=0&f_wma=1&f_dir=0&f_fla=1&f_java=1&f_gears=0&f_ag=1&res=1920x1080&cd=24&cookie=1&tz=Europe%2FLondon&url=http%3A%2F%2Fyalisassoon.github.com%2Fcl-collector-tests%2Fasync.html&nuid=7fc17b64-b202-46e4-8d3a-4d144edf2b23  - - -"
  val expected = new SnowPlowEvent().tap { e =>
    e.user_id = "alex@snowplowanalytics.com"
    e.domain_userid = "915bc1dd0a4c5ba5"
    e.network_userid = "7fc17b64-b202-46e4-8d3a-4d144edf2b23"
  }

  "For a row containing uid, nuid and duid, the ETL" should {

    val actual = SnowPlowDeserializer.deserialize(row)

    "have user_id (Business User ID) = %s".format(expected.user_id) in {
      actual.user_id must_== expected.user_id
    }
    "have domain_userid (Domain User ID) = %s".format(expected.domain_userid) in {
      actual.domain_userid must_== expected.domain_userid
    }
    "have network_userid (Network User ID) = %s".format(expected.network_userid) in {
      actual.network_userid must_== expected.network_userid
    }
  }
}