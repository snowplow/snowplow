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

// Java
import java.util.UUID

// Specs2
import org.specs2.mutable.Specification

// Test suite helpers
import test.SnowPlowTest

class EventIdTest extends Specification {

  "generateEventId()" should {
    "generate a String" in {
      SnowPlowEventStruct.generateEventId must beAnInstanceOf[String]
    }
  }

  "generateEventId()" should {
    "generate a UUID-format event ID" in {
      val eventId = SnowPlowEventStruct.generateEventId
      SnowPlowTest.stringlyTypedUuid(eventId) must_== eventId
    }
  }

  // TODO: add in some tests for stringToBool
}
