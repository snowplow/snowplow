/*
 * Copyright (c) 2012 SnowPlow Analytics Ltd. All rights reserved.
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

class ConversionTest extends Specification {

  "The string \"-\"" should {
    "be identified as null by isNullField" in {
     SnowPlowEventStruct.isNullField("-") must beTrue
    }
  }

  "The string \"\"" should {
    "be identified as null by isNullField" in {
     SnowPlowEventStruct.isNullField("") must beTrue
    }
  }

  "The string <<null>>" should {
    "be identified as null by isNullField" in {
     SnowPlowEventStruct.isNullField(null) must beTrue
    }
  }

  "The string \"-a-\"" should {
    "NOT be identified as null by isNullField" in {
     SnowPlowEventStruct.isNullField("-a-") must beFalse
    }
  }

  // TODO: add in some tests for stringToBool
}
