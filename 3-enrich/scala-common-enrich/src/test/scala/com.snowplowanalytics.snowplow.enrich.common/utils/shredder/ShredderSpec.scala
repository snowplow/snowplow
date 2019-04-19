/*
 * Copyright (c) 2014-2019 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.enrich.common
package utils.shredder

import org.specs2.Specification

import outputs.EnrichedEvent
import utils.Clock._

class ShredderSpec extends Specification {
  def is = s2"""
  This is a specification to test the Shredder functionality
  makePartialHierarchy should initialize a partial TypeHierarchy                   $e1
  shred should extract the JSONs from an unstructured event with multiple contexts $e2
  """

  val EventId = "f81d4fae-7dec-11d0-a765-00a0c91e6bf6"
  val CollectorTimestamp = "2014-04-29 09:00:54.000"

  def e1 =
    Shredder.makePartialHierarchy(EventId, CollectorTimestamp) must_==
      TypeHierarchy(
        rootId = EventId,
        rootTstamp = CollectorTimestamp,
        refRoot = "events",
        refTree = List("events"),
        refParent = "events"
      )

  def e2 = {
    val event = {
      val e = new EnrichedEvent()
      e.event_id = EventId
      e.collector_tstamp = CollectorTimestamp
      e.unstruct_event =
        """{"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0","data":{"schema":"iglu:com.snowplowanalytics.snowplow/link_click/jsonschema/1-0-0","data":{"targetUrl":"http://snowplowanalytics.com/blog/page2","elementClasses":["next"]}}}"""
      e.contexts =
        """{"schema":"iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-0","data":[{"schema":"iglu:org.schema/WebPage/jsonschema/1-0-0","data":{"datePublished":"2014-07-23T00:00:00Z","author":"Jonathan Almeida","inLanguage":"en-US","genre":"blog","breadcrumb":["blog","releases"],"keywords":["snowplow","analytics","java","jvm","tracker"]}},{"schema":"iglu:org.schema/WebPage/jsonschema/1-0-0","data":{"datePublished":"2014-07-23T00:00:00Z","author":"Jonathan Almeida","inLanguage":"en-US","genre":"blog","breadcrumb":["blog","releases"],"keywords":["snowplow","analytics","java","jvm","tracker"]}}]}"""
      e
    }

    // TODO: check actual contents (have already confirmed in REPL)
    //println(Shredder.shred(event, SpecHelpers.client).value)
    Shredder.shred(event, SpecHelpers.client).value.toOption.get must have size (3)
  }
}
