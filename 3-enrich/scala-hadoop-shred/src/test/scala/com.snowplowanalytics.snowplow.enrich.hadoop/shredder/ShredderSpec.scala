/*
 * Copyright (c) 2012-2014 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.enrich
package hadoop
package shredder

// Specs2
import org.specs2.Specification

class ShredderSpec extends Specification /*with DataTables with ValidationMatchers*/ { def is =

  "This is a specification to test the Shredder functionality"                 ^
                                                                              p^
  "makePartialHierarchy should initialize a partial TypeHierarchy"             ! e1^
                                                                               end 

  val EventId = "f81d4fae-7dec-11d0-a765-00a0c91e6bf6"
  val CollectorTimestamp = "2014-04-29 09:00:54.000"

  def e1 =
    Shredder.makePartialHierarchy(EventId, CollectorTimestamp) must_==
      TypeHierarchy(
        rootId =      EventId,
        rootTstamp =  CollectorTimestamp,
        refRoot =    "events",
        refTree =     List("events"),
        refParent =  "events")
}
