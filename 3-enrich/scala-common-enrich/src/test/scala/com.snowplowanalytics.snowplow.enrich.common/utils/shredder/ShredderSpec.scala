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
package com.snowplowanalytics
package snowplow
package enrich
package common
package utils
package shredder

// Snowplow Utils
import util.Tap._

// Snowplow Common Enrich
import outputs.EnrichedEvent

// Specs2
import org.specs2.Specification

class ShredderSpec extends Specification /*with DataTables with ValidationMatchers*/ { def is =

  "This is a specification to test the Shredder functionality"                       ^
                                                                                    p^
  "makePartialHierarchy should initialize a partial TypeHierarchy"                   ! e1^
  "shred should extract the JSONs from an unstructured event with multiple contexts" ! e2^
                                                                                     end

  val EventId = "f81d4fae-7dec-11d0-a765-00a0c91e6bf6"
  val CollectorTimestamp = "2014-04-29 09:00:54.000"

  implicit val resolver = SpecHelpers.IgluResolver

  def e1 =
    Shredder.makePartialHierarchy(EventId, CollectorTimestamp) must_==
      TypeHierarchy(
        rootId =      EventId,
        rootTstamp =  CollectorTimestamp,
        refRoot =    "events",
        refTree =     List("events"),
        refParent =  "events")

  def e2 = {
    val event = new EnrichedEvent().tap { e =>
        e.event_id = EventId
        e.collector_tstamp = CollectorTimestamp
        e.unstruct_event = """{"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0","data":{"schema":"iglu:com.snowplowanalytics.snowplow/link_click/jsonschema/1-0-0","data":{"targetUrl":"http://snowplowanalytics.com/blog/page2","elementClasses":["next"]}}}"""
        e.contexts = """{"schema":"iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-0","data":[{"schema":"iglu:org.schema/WebPage/jsonschema/1-0-0","data":{"datePublished":"2014-07-23T00:00:00Z","author":"Jonathan Almeida","inLanguage":"en-US","genre":"blog","breadcrumb":["blog","releases"],"keywords":["snowplow","analytics","java","jvm","tracker"]}},{"schema":"iglu:org.schema/WebPage/jsonschema/1-0-0","data":{"datePublished":"2014-07-23T00:00:00Z","author":"Jonathan Almeida","inLanguage":"en-US","genre":"blog","breadcrumb":["blog","releases"],"keywords":["snowplow","analytics","java","jvm","tracker"]}}]}"""
      }

    // TODO: check actual contents (have already confirmed in REPL)
    Shredder.shred(event).toOption.get must have size(3)
  }
}
