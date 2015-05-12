/*
 * Copyright (c) 2013-2014 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0, and
 * you may not use this file except in compliance with the Apache License
 * Version 2.0.  You may obtain a copy of the Apache License Version 2.0 at
 * http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the Apache License Version 2.0 is distributed on an "AS
 * IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the Apache License Version 2.0 for the specific language
 * governing permissions and limitations there under.
 */
package com.snowplowanalytics.snowplow
package collectors
package scalastream
package sinks

// Specs2
import org.specs2.mutable.Specification

class SplitBatchSpec extends Specification {
  val splitBatch = SplitBatch

  "SplitBatch.split" should {

    "Batch a list of strings based on size" in {

      splitBatch.split(List("a", "b", "c"), 5, 1) must_==
        SplitBatch(
          List(List("c"),List("b", "a")),
          Nil)

    }

    "Reject only those strings which are too big" in {

      splitBatch.split(List("123456", "1", "123"), 5, 0) must_==
        SplitBatch(
          List(List("123", "1")),
          List("123456"))
    }

    "Batch a long list of strings" in {

      splitBatch.split(List("12345677890", "123456789", "12345678", "1234567", "123456", "12345", "1234", "123", "12", "1"), 9, 0) must_==
        SplitBatch(
          List(
            List("1", "12", "123"),
            List("1234", "12345"),
            List("123456"),
            List("1234567"),
            List("12345678"),
            List("123456789")),
          List("12345677890"))
    }
  }

}
