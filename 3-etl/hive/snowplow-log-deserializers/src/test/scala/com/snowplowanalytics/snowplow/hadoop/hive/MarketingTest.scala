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

// Scala
import scala.collection.JavaConversions

// Specs2
import org.specs2.mutable.Specification

class MarketingTest extends Specification {

  // Toggle if tests are failing and you want to inspect the struct contents
  val DEBUG = false;

  // TODO: generalise this so every test can use it (and add in the other fields)
  case class SnowPlowEvent(
    mkt_medium: String,
    mkt_source: String,
    mkt_term: String,
    mkt_content: String,
    mkt_campaign: String
  ) 

  val testData = List("TODO" ->
                        SnowPlowEvent("TODO", "TODO", "TODO", "TODO", "TODO"),
                      "TODO" ->
                        SnowPlowEvent("TODO", "TODO", "TODO", "TODO", "TODO"),
                      "TODO" ->
                        SnowPlowEvent("TODO", "TODO", "TODO", "TODO", "TODO")
                     )

  "A SnowPlow event with marketing fields should have this marketing data extracted" >> {
    testData foreach { case (row, expected) =>

       val actual = SnowPlowEventDeserializer.deserializeLine(row, DEBUG).asInstanceOf[SnowPlowEventStruct]

      "The SnowPlow row \"%s\"".format(row) should {
        "have mkt_medium (Medium) = %s".format(expected.mkt_medium) in {
          actual.mkt_medium must_== expected.mkt_medium
        }
        // TODO: add other four fields
      }
    }
  }
}
