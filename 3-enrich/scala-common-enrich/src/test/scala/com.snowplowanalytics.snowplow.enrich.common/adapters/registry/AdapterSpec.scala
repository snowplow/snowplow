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
package com.snowplowanalytics
package snowplow
package enrich
package common
package adapters
package registry

// Iglu
import iglu.client.Resolver

// Joda-Time
import org.joda.time.DateTime

// Scalaz
import scalaz._
import Scalaz._

// json4s
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

// Snowplow
import SpecHelpers._
import loaders.{
  CollectorApi,
  CollectorSource,
  CollectorContext,
  CollectorPayload
}

// Specs2
import org.specs2.{Specification, ScalaCheck}
import org.specs2.matcher.DataTables
import org.specs2.scalaz.ValidationMatchers

class AdapterSpec extends Specification with DataTables with ValidationMatchers with ScalaCheck { def is =

  "This is a specification to test the Adapter trait's functionality"                                                  ^
                                                                                                                      p^
  "toMap should convert a list of name-value pairs into a map"                                                         ! e1^
  "toUnstructEventParams should generate a boilerplate set of parameters for an empty unstructured event"              ! e2^
  "toUnstructEventParams should preserve nuid, aid, cv and p outside of the unstructured event"                        ! e3^
  "lookupSchema must return a Success Nel for a valid key being passed against an event-schema map"                    ! e4^
  "lookupSchema must return a Failure Nel for an invalid key being passed against an event-schema map"                 ! e5^
  "lookupSchema must return a Failure Nel with an index if one is passed to it"                                        ! e6^
  "rawEventsListProcessor must return a Failure Nel if there are any Failures in the list"                             ! e7^
  "rawEventsListProcessor must return a Success Nel of RawEvents if the list is full of success"                       ! e8^                           
                                                                                                                       end
  // TODO: add test for buildFormatter()

  implicit val resolver = SpecHelpers.IgluResolver

  object BaseAdapter extends Adapter {
    def toRawEvents(payload: CollectorPayload)(implicit resolver: Resolver) = "Base".failNel
  }

  object Shared {
    val api = CollectorApi("com.adapter", "v1")
    val cljSource = CollectorSource("clj-tomcat", "UTF-8", None)
    val context = CollectorContext(DateTime.parse("2013-08-29T00:18:48.000+00:00").some, "37.157.33.123".some, None, None, Nil, None)
    val contentType = "application/x-www-form-urlencoded"
  }

  private val SchemaMap = Map (
    "adapterTest" -> "iglu:com.adaptertest/test/jsonschema/1-0-0"
  )

  def e1 = {
    val pairs = toNameValuePairs("a" -> "1", "b" -> "2", "c" -> "3")
    BaseAdapter.toMap(pairs) must_== Map("a" -> "1", "b" -> "2", "c" -> "3")
  }

  def e2 = {
    val params = BaseAdapter.toUnstructEventParams("tv", Map[String, String](), "iglu:foo", _ => List[JField](), "app")
    params must_== Map(
      "tv"    -> "tv",
      "e"     -> "ue",
      "p"     -> "app",
      "ue_pr" -> """{"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0","data":{"schema":"iglu:foo","data":{}}}"""
    )
  }

  def e3 = {
    val shared = Map("nuid" -> "123", "aid" -> "42", "cv" -> "clj-tomcat", "p" -> "srv")
    val params = BaseAdapter.toUnstructEventParams("tv", shared, "iglu:foo", _ => List[JField](), "app")
    params must_== shared ++ Map(
      "tv"    -> "tv",
      "e"     -> "ue",
      "ue_pr" -> """{"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0","data":{"schema":"iglu:foo","data":{}}}"""
    )
  }

  def e4 = {
    val expected = "iglu:com.adaptertest/test/jsonschema/1-0-0"
    BaseAdapter.lookupSchema("adapterTest".some, "Adapter", SchemaMap) must beSuccessful(expected)
  }

  def e5 = 
    "SPEC NAME"                 || "SCHEMA TYPE"      | "EXPECTED OUTPUT"                                                                   |
    "Failing, nothing passed"   !! None               ! "Adapter event failed: type parameter not provided - cannot determine event type" |
    "Failing, empty type"       !! Some("")           ! "Adapter event failed: type parameter is empty - cannot determine event type"     |
    "Failing, bad type passed"  !! Some("bad")        ! "Adapter event failed: type parameter [bad] not recognized"                       |> {
      (_, et, expected) => BaseAdapter.lookupSchema(et, "Adapter", SchemaMap) must beFailing(NonEmptyList(expected))
  }

  def e6 = {
    val expected = "Adapter event at index [2] failed: type parameter not provided - cannot determine event type"
    BaseAdapter.lookupSchema(None, "Adapter", 2, SchemaMap) must beFailing(NonEmptyList(expected))
  }

  def e7 = {
    val rawEvent = RawEvent(Shared.api, Map("tv" -> "com.adapter-v1", "e" -> "ue", "p" -> "srv"), Shared.contentType.some, Shared.cljSource, Shared.context)
    val validatedRawEventsList = List(Success(rawEvent), Failure(NonEmptyList("This is a failure string-1")), Failure(NonEmptyList("This is a failure string-2")))
    val expected = NonEmptyList("This is a failure string-1", "This is a failure string-2")
    BaseAdapter.rawEventsListProcessor(validatedRawEventsList) must beFailing(expected)
  }

  def e8 = {
    val rawEvent = RawEvent(Shared.api, Map("tv" -> "com.adapter-v1", "e" -> "ue", "p" -> "srv"), Shared.contentType.some, Shared.cljSource, Shared.context)
    val validatedRawEventsList = List(Success(rawEvent),Success(rawEvent),Success(rawEvent))
    val expected = NonEmptyList(RawEvent(Shared.api, Map("tv" -> "com.adapter-v1", "e" -> "ue", "p" -> "srv"), Shared.contentType.some, Shared.cljSource, Shared.context),RawEvent(Shared.api, Map("tv" -> "com.adapter-v1", "e" -> "ue", "p" -> "srv"), Shared.contentType.some, Shared.cljSource, Shared.context),RawEvent(Shared.api, Map("tv" -> "com.adapter-v1", "e" -> "ue", "p" -> "srv"), Shared.contentType.some, Shared.cljSource, Shared.context))
    BaseAdapter.rawEventsListProcessor(validatedRawEventsList) must beSuccessful(expected)
  }
}
