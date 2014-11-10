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
package com.snowplowanalytics.snowplow.enrich.common
package adapters
package registry

// Joda-Time
import org.joda.time.DateTime

// Scalaz
import scalaz._
import Scalaz._

// json4s
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import org.json4s.scalaz.JsonScalaz._

// Snowplow
import loaders.{
  CollectorApi,
  CollectorSource,
  CollectorContext,
  CollectorPayload
}
import utils.ConversionUtils
import SpecHelpers._

// Specs2
import org.specs2.{Specification, ScalaCheck}
import org.specs2.matcher.DataTables
import org.specs2.scalaz.ValidationMatchers

class MandrillAdapterSpec extends Specification with DataTables with ValidationMatchers with ScalaCheck { def is =

  "This is a specification to test the MandrillAdapter functionality"                                                  ^
                                                                                                                       p^
  "extractKeyValueFromJson must return an Option[String] or None if it can or cannot find a valid value"            ! e1^
  "lookupSchema must return a Success String for a valid event type"                                                ! e2^
  "lookupSchema must return a Failure String if the event type is not valid"                                        ! e3^
  "jsonToRawEvent must return a Success RawEvent if the JSON passed is valid"                                       ! e4^
  "jsonToRawEvent must return a Failure Nel if the JSON does not contain the 'event' key or an invalid event type"  ! e5^
  "bodyToEventList must return a Success List[JValue] for a valid events string"                                    ! e6^
  "bodyToEventList must return a Failure String if the mapped events string is not in a valid format"               ! e7^
  "bodyToEventList must return a Failure String if the event string could not be parsed into JSON"                  ! e8^
  //"toRawEvents must return a Success Nel for a valid payload.body being passed"                                     ! e9^
  "toRawEvents must return a Failure Nel if the payload body is empty"                                              ! e10^
  "toRawEvents must return a Failure Nel if the payload content type is empty"                                      ! e11^
  "toRawEvents must return a Failure Nel if the payload content type does not match expectation"                    ! e12^
  //"toRawEvents must return a Failure Nel if any Failure's occured with any other function used within toRawEvents"  ! e15^
                                                                                                                       end
  implicit val resolver = SpecHelpers.IgluResolver

  object Shared {
    val api = CollectorApi("com.mandrill", "v1")
    val cljSource = CollectorSource("clj-tomcat", "UTF-8", None)
    val context = CollectorContext(DateTime.parse("2013-08-29T00:18:48.000+00:00"), "37.157.33.123".some, None, None, Nil, None)
  }

  val ContentType = "application/x-www-form-urlencoded"

  def e1 = 
    "SPEC NAME"               || "KEY"   | "JSON"                             | "EXPECTED OUTPUT"  |
    "Valid, value found"      !! "event" ! parse("""{"event":"subscribe"}""") ! Some("subscribe")  |
    "Invalid, no value found" !! "none"  ! parse("""{"event":"subscribe"}""") ! None               |> {
      (_, key, json, expected) => MandrillAdapter.extractKeyValueFromJson(key,json) mustEqual expected
  }

  def e2 = 
    "SPEC NAME"                         || "SCHEMA TYPE" | "EXPECTED OUTPUT"                                           |
    "Valid, type MessageBounced"        !! "hard_bounce" ! "iglu:com.mandrill/message_bounced/jsonschema/1-0-0"        |
    "Valid, type MessageClicked"        !! "click"       ! "iglu:com.mandrill/message_clicked/jsonschema/1-0-0"        |
    "Valid, type MessageDelayed"        !! "deferral"    ! "iglu:com.mandrill/message_delayed/jsonschema/1-0-0"        |
    "Valid, type MessageMarkedAsSpam"   !! "spam"        ! "iglu:com.mandrill/message_marked_as_spam/jsonschema/1-0-0" |
    "Valid, type MessageOpened"         !! "open"        ! "iglu:com.mandrill/message_opened/jsonschema/1-0-0"         |
    "Valid, type MessageRejected"       !! "reject"      ! "iglu:com.mandrill/message_rejected/jsonschema/1-0-0"       |
    "Valid, type MessageSent"           !! "send"        ! "iglu:com.mandrill/message_sent/jsonschema/1-0-0"           |
    "Valid, type MessageSoftBounced"    !! "soft_bounce" ! "iglu:com.mandrill/message_soft_bounced/jsonschema/1-0-0"   |
    "Valid, type RecipientUnsubscribed" !! "unsub"       ! "iglu:com.mandrill/recipient_unsubscribed/jsonschema/1-0-0" |> {
      (_, schema, expected) => MandrillAdapter.lookupSchema(schema) must beSuccessful(expected)
  }

  def e3 = 
    "SPEC NAME"               || "SCHEMA TYPE"  | "EXPECTED OUTPUT"                                                |
    "Invalid, bad type"       !! "bad"          ! "Mandrill event parameter [bad] not recognized"                  |
    "Invalid, no type"        !! ""             ! "Mandrill event parameter is empty: cannot determine event type" |> {
      (_, schema, expected) => MandrillAdapter.lookupSchema(schema) must beFailing(expected)
  }

  def e4 = {
    val json = JObject(List(("event",JString("click"))))  
    val payload = CollectorPayload(Shared.api, Nil, ContentType.some, None, Shared.cljSource, Shared.context)
    val expectedMap = Map("tv" -> "com.mandrill-v1", "e" -> "ue", "p" -> "srv", "ue_pr" -> """{"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0","data":{"schema":"iglu:com.mandrill/message_clicked/jsonschema/1-0-0","data":{"event":"click"}}}""")
    val actual = MandrillAdapter.jsonToRawEvent(payload, json)
    actual must beSuccessful(RawEvent(Shared.api, expectedMap, ContentType.some, Shared.cljSource, Shared.context))
  }

  def e5 = 
    "SPEC NAME"                      || "JSON"                                       | "EXPECTED OUTPUT"                                                    |
    "Invalid, no event key"          !! JObject(List(("type",JString("subscribe")))) ! "Mandrill event parameter not provided: cannot determine event type" |
    "Invalid, event value not valid" !! JObject(List(("event",JString("bad"))))      ! "Mandrill event parameter [bad] not recognized"                      |> {
      (_, json, expected) => {
        val payload = CollectorPayload(Shared.api, Nil, ContentType.some, None, Shared.cljSource, Shared.context)
        val actual = MandrillAdapter.jsonToRawEvent(payload, json)
        actual must beFailing(NonEmptyList(expected))
      }
  }

  def e6 = {
    val bodyStr = "mandrill_events=%5B%7B%22event%22%3A%20%22subscribe%22%7D%5D"
    val expected = List(JObject(List(("event",JString("subscribe")))))
    val actual = MandrillAdapter.bodyToEventList(bodyStr)
    actual must beSuccessful(expected)
  }

  def e7 = 
    "SPEC NAME"                         || "STRING TO PROCESS"                        | "EXPECTED OUTPUT"                                                      |
    "Failure, empty events string"      !! "mandrill_events="                         ! "Mandrill events string is empty: nothing to process"                  |
    "Failure, too many key-value pairs" !! "mandrill_events=some&mandrill_extra=some" ! "Mapped Mandrill body has invalid count of keys"                       |
    "Failure, incorrect key"            !! "events_mandrill=something"                ! "Mapped Mandrill body does not have 'mandrill_events' as its only key" |> {
      (_, str, expected) => MandrillAdapter.bodyToEventList(str) must beFailing(expected)
  }

  def e8 = {
    val bodyStr = "mandrill_events=%5B%7B%22event%22%3A%22click%7D%5D"
    val expected = ""
    val actual = MandrillAdapter.bodyToEventList(bodyStr)
    actual must beFailing(expected)
  }

  def e10 = {
    val payload = CollectorPayload(Shared.api, Nil, ContentType.some, None, Shared.cljSource, Shared.context)
    val actual = MandrillAdapter.toRawEvents(payload)
    actual must beFailing(NonEmptyList("Request body is empty: no Mandrill events to process"))
  }

  def e11 = {
    val body = "mandrill_events=%5B%7B%22event%22%3A%20%22subscribe%22%7D%5D"
    val payload = CollectorPayload(Shared.api, Nil, None, body.some, Shared.cljSource, Shared.context)
    val actual = MandrillAdapter.toRawEvents(payload)
    actual must beFailing(NonEmptyList("Request body provided but content type empty, expected application/x-www-form-urlencoded for Mandrill"))
  }

  def e12 = {
    val body = "mandrill_events=%5B%7B%22event%22%3A%20%22subscribe%22%7D%5D"
    val ct = "application/x-www-form-urlencoded; charset=utf-8"
    val payload = CollectorPayload(Shared.api, Nil, ct.some, body.some, Shared.cljSource, Shared.context)
    val actual = MandrillAdapter.toRawEvents(payload)
    actual must beFailing(NonEmptyList("Content type of application/x-www-form-urlencoded; charset=utf-8 provided, expected application/x-www-form-urlencoded for Mandrill"))
  }
}
