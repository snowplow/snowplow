/*
 * Copyright (c) 2014 Snowplow Analytics Ltd. All rights reserved.
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

class TypeHierarchySpec extends Specification { def is =

  "This is a specification to test the TypeHierarchy case class"               ^
                                                                              p^
  "a TypeHierarchy should be convertible to JSON"                              ! e1^
  "the complete method should finalize a partial TypeHierarchy"                ! e2^
                                                                               end 

  val EventId = "f81d4fae-7dec-11d0-a765-00a0c91e6bf6"
  val CollectorTimestamp = "2014-04-29 09:00:54.000"

  def e1 = {
    val hierarchy =
      TypeHierarchy(
        rootId =      EventId,
        rootTstamp =  CollectorTimestamp,
        refRoot =    "events",
        refTree =     List("events", "new_ticket"),
        refParent =  "events")

    // TODO: add missing refTree
    hierarchy.toJsonNode.toString must_== s"""{"rootId":"${EventId}","rootTstamp":"${CollectorTimestamp}","refRoot":"events","refTree":["events","new_ticket"],"refParent":"events"}"""
  }

  def e2 = {
    val partial = Shredder.makePartialHierarchy(EventId, CollectorTimestamp)
    
    partial.complete(List("link_click", "elementClasses")) must_==
      TypeHierarchy(
        rootId =      EventId,
        rootTstamp =  CollectorTimestamp,
        refRoot =    "events",
        refTree =     List("events", "link_click", "elementClasses"),
        refParent =  "link_click")
  }
}
