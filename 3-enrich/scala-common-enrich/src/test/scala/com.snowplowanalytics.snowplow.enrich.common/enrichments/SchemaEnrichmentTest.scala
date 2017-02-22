/*
 * Copyright (c) 2012-2015 Snowplow Analytics Ltd. All rights reserved.
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
package common
package enrichments

// Iglu
import com.snowplowanalytics.iglu.client.SchemaKey

// Common
import outputs.EnrichedEvent
import enrichments.SchemaEnrichment._

// Specs2
import org.specs2.Specification
import org.specs2.matcher.DataTables
import org.specs2.scalaz.ValidationMatchers

// Scalaz
import scalaz._
import Scalaz._

class SchemaEnrichmentTest extends Specification with DataTables with ValidationMatchers {

  implicit val resolver = SpecHelpers.IgluResolver
  val signupFormSubmitted = """{"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0","data":{"schema":"iglu:com.snowplowanalytics.snowplow-website/signup_form_submitted/jsonschema/1-0-0","data":{"name":"Χαριτίνη NEW Unicode test","email":"alex+test@snowplowanalytics.com","company":"SP","eventsPerMonth":"< 1 million","serviceType":"unsure"}}}"""
  val invalidPayload = """{"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0","data":{"schema":"iglu:com.snowplowanalytics.snowplow-website/signup_form_submitted/jsonschema/1-0-0","data":{"serviceType":"unsure"}}}"""

  def is = s2"""
    Extracting SchemaKeys from valid events should work $e1
    Invalid events should fail when extracting SchemaKeys $e2
  """

  def e1 =
      "SPEC NAME"              || "EVENT"                            | "EXPECTED SCHEMA"          |
      "page view"              !! event("page_view")                 ! SchemaKey("com.snowplowanalytics.snowplow", "page_view", "jsonschema", "1-0-0")             |
      "ping ping"              !! event("page_ping")                 ! SchemaKey("com.snowplowanalytics.snowplow", "page_ping", "jsonschema", "1-0-0")             |
      "transaction"            !! event("transaction")               ! SchemaKey("com.snowplowanalytics.snowplow", "transaction", "jsonschema", "1-0-0")          |
      "transaction item"       !! event("transaction_item")          ! SchemaKey("com.snowplowanalytics.snowplow", "transaction_item", "jsonschema", "1-0-0")      |
      "struct event"           !! event("struct")                    ! SchemaKey("com.google.analytics", "event", "jsonschema", "1-0-0")               |
      "invalid unstruct event" !! unstructEvent(invalidPayload)      ! SchemaKey("com.snowplowanalytics.snowplow-website", "signup_form_submitted", "jsonschema", "1-0-0")  |
      "unstruct event"         !! unstructEvent(signupFormSubmitted) ! SchemaKey("com.snowplowanalytics.snowplow-website", "signup_form_submitted", "jsonschema", "1-0-0")  |> {
      (_, event, expected) => {
        val schema = SchemaEnrichment.extractSchema(event)
        schema must beSuccessful(expected)
      }
    }

  val nonSchemedPayload = """{"name":"Χαριτίνη NEW Unicode test","email":"alex+test@snowplowanalytics.com","company":"SP","eventsPerMonth":"< 1 million","serviceType":"unsure"}"""
  val invalidKeyPayload = """{"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0","data":{"schema":"iglu:com.snowplowanalytics.snowplow-website/signup_form_submitted/jsonschema","data":{"name":"Χαριτίνη NEW Unicode test","email":"alex+test@snowplowanalytics.com","company":"SP","eventsPerMonth":"< 1 million","serviceType":"unsure"}}}"""

  def e2 =
    "SPEC NAME"           || "EVENT"                          |
      "unknown event"     !! event("unknown")                 |
      "missing event"     !! event(null)                      |
      "not schemed"       !! unstructEvent(nonSchemedPayload) |
      "invalid key"       !! unstructEvent(invalidKeyPayload) |> {
      (_, event) => {
        val schema = SchemaEnrichment.extractSchema(event)
        schema must beFailing
      }
    }

  def event(eventType: String) = {
    val event: EnrichedEvent = new EnrichedEvent()
    event.setEvent(eventType)
    event
  }

  def unstructEvent(unstruct: String) = {
    val event: EnrichedEvent = new EnrichedEvent()
    event.setEvent("unstruct")
    event.setUnstruct_event(unstruct)
    event
  }
}
