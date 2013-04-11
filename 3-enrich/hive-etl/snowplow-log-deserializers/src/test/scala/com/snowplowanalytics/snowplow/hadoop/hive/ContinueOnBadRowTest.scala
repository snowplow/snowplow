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

// Hive
import org.apache.hadoop.hive.serde2.SerDeException

// Deserializer
import test.SnowPlowDeserializer

/**
 * Tests the serde's continue_on_unexpected_error setting
 */
class ContinueOnBadRowTest extends Specification {

  // Toggle if tests are failing and you want to inspect the struct contents
  implicit val _DEBUG = false

  val malformedRows = Seq(
    "",
    "NOT VALID",
    "2012-05-21\t07:14:47\tFRA2\t3343\t83.4.209.35\tGET\td3t05xllj8hhgj.cloudfront.net"
  )

  "If continue_on_unexpected_error is set, an invalid or corrupted CloudFront row should *not* throw an exception" >> {
     malformedRows foreach { row =>
      "invalid row \"%s\" returns <<null>>" in {
        SnowPlowDeserializer.deserializeUntyped(row, continueOn=true) must beNull
      }
    }
  }
}
