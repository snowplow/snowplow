/*
 * Copyright (c) 2012-2019 Snowplow Analytics Ltd. All rights reserved.
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

import cats.Monad
import cats.data.{NonEmptyList, Validated}
import cats.effect.Clock
import cats.syntax.option._
import cats.syntax.validated._
import com.snowplowanalytics.iglu.client.Client
import com.snowplowanalytics.iglu.client.resolver.registries.RegistryLookup
import io.circe._
import io.circe.literal._
import org.joda.time.DateTime
import org.specs2.Specification
import org.specs2.matcher.{DataTables, ValidatedMatchers}

import SpecHelpers._
import loaders.{CollectorApi, CollectorContext, CollectorPayload, CollectorSource}

class AdapterSpec extends Specification with DataTables with ValidatedMatchers {
  def is = s2"""
  This is a specification to test the Adapter trait's functionality
  toMap should convert a list of name-value pairs into a map                                                 $e1
  toUnstructEventParams should generate a boilerplate set of parameters for an empty unstructured event      $e2
  toUnstructEventParams should preserve nuid, aid, cv, url, eid, ttm and p outside of the unstructured event $e3
  lookupSchema must return a Validated.Valid Nel for a valid key being passed against an event-schema map            $e4
  lookupSchema must return a Validated.Invalid Nel for an invalid key being passed against an event-schema map         $e5
  lookupSchema must return a Validated.Invalid Nel with an index if one is passed to it                                $e6
  rawEventsListProcessor must return a Validated.Invalid Nel if there are any Validated.Invalids in the list                     $e7
  rawEventsListProcessor must return a Validated.Valid Nel of RawEvents if the list is full of success               $e8
  cleanupJsonEventValues must clean 'ts':[JInt, JString] fields into to a valid JsonSchema date-time format  $e9
  cleanupJsonEventValues must remove key-pairs if specified                                                  $e10
  """

  // TODO: add test for buildFormatter()

  object BaseAdapter extends Adapter {
    def toRawEvents[F[_]: Monad: RegistryLookup: Clock](
      payload: CollectorPayload, client: Client[F, Json]) = Monad[F].pure("Base".invalidNel)
  }

  object Shared {
    val api = CollectorApi("com.adapter", "v1")
    val cljSource = CollectorSource("clj-tomcat", "UTF-8", None)
    val context = CollectorContext(
      DateTime.parse("2013-08-29T00:18:48.000+00:00").some,
      "37.157.33.123".some,
      None,
      None,
      Nil,
      None)
    val contentType = "application/x-www-form-urlencoded"
  }

  private val SchemaMap = Map(
    "adapterTest" -> "iglu:com.adaptertest/test/jsonschema/1-0-0"
  )

  def e1 = {
    val pairs = toNameValuePairs("a" -> "1", "b" -> "2", "c" -> "3")
    BaseAdapter.toMap(pairs) must_== Map("a" -> "1", "b" -> "2", "c" -> "3")
  }

  def e2 = {
    val params = BaseAdapter.toUnstructEventParams(
      "tv",
      Map[String, String](),
      "iglu:foo",
      _ => Json.fromJsonObject(JsonObject.empty),
      "app")
    params must_== Map(
      "tv" -> "tv",
      "e" -> "ue",
      "p" -> "app",
      "ue_pr" -> """{"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0","data":{"schema":"iglu:foo","data":{}}}"""
    )
  }

  def e3 = {
    val shared = Map(
      "nuid" -> "123",
      "aid" -> "42",
      "cv" -> "clj-tomcat",
      "p" -> "srv",
      "eid" -> "321",
      "ttm" -> "2015-11-13T16:31:52.393Z",
      "url" -> "http://localhost")
    val params = BaseAdapter.toUnstructEventParams(
      "tv",
      shared,
      "iglu:foo",
      _ => Json.fromJsonObject(JsonObject.empty),
      "app")
    params must_== shared ++ Map(
      "tv" -> "tv",
      "e" -> "ue",
      "eid" -> "321",
      "ttm" -> "2015-11-13T16:31:52.393Z",
      "url" -> "http://localhost",
      "ue_pr" -> """{"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0","data":{"schema":"iglu:foo","data":{}}}"""
    )
  }

  def e4 = {
    val expected = "iglu:com.adaptertest/test/jsonschema/1-0-0"
    BaseAdapter.lookupSchema("adapterTest".some, "Adapter", SchemaMap) must beRight(expected)
  }

  def e5 =
    "SPEC NAME" || "SCHEMA TYPE" | "EXPECTED OUTPUT" |
      "Failing, nothing passed" !! None ! "Adapter event failed: type parameter not provided - cannot determine event type" |
      "Failing, empty type" !! Some("") ! "Adapter event failed: type parameter is empty - cannot determine event type" |
      "Failing, bad type passed" !! Some("bad") ! "Adapter event failed: type parameter [bad] not recognized" |> {
      (_, et, expected) =>
        BaseAdapter.lookupSchema(et, "Adapter", SchemaMap) must beLeft(expected)
    }

  def e6 = {
    val expected =
      "Adapter event at index [2] failed: type parameter not provided - cannot determine event type"
    BaseAdapter.lookupSchema(None, "Adapter", 2, SchemaMap) must beLeft(expected)
  }

  def e7 = {
    val rawEvent = RawEvent(
      Shared.api,
      Map("tv" -> "com.adapter-v1", "e" -> "ue", "p" -> "srv"),
      Shared.contentType.some,
      Shared.cljSource,
      Shared.context)
    val validatedRawEventsList =
      List(
        Validated.Valid(rawEvent),
        Validated.Invalid(NonEmptyList.one("This is a failure string-1")),
        Validated.Invalid(NonEmptyList.one("This is a failure string-2"))
      )
    val expected = NonEmptyList.of("This is a failure string-1", "This is a failure string-2")
    BaseAdapter.rawEventsListProcessor(validatedRawEventsList) must beInvalid(expected)
  }

  def e8 = {
    val rawEvent = RawEvent(
      Shared.api,
      Map("tv" -> "com.adapter-v1", "e" -> "ue", "p" -> "srv"),
      Shared.contentType.some,
      Shared.cljSource,
      Shared.context)
    val validatedRawEventsList =
      List(Validated.Valid(rawEvent), Validated.Valid(rawEvent), Validated.Valid(rawEvent))
    val expected = NonEmptyList.of(
      RawEvent(
        Shared.api,
        Map("tv" -> "com.adapter-v1", "e" -> "ue", "p" -> "srv"),
        Shared.contentType.some,
        Shared.cljSource,
        Shared.context),
      RawEvent(
        Shared.api,
        Map("tv" -> "com.adapter-v1", "e" -> "ue", "p" -> "srv"),
        Shared.contentType.some,
        Shared.cljSource,
        Shared.context),
      RawEvent(
        Shared.api,
        Map("tv" -> "com.adapter-v1", "e" -> "ue", "p" -> "srv"),
        Shared.contentType.some,
        Shared.cljSource,
        Shared.context)
    )
    BaseAdapter.rawEventsListProcessor(validatedRawEventsList) must beValid(expected)
  }

  def e9 =
    "SPEC NAME" || "JSON" | "EXPECTED OUTPUT" |
      "Change one value" !! json"""{"ts":1415709559}""" ! json"""{ "ts": "2014-11-11T12:39:19.000Z" }""" |
      "Change multiple values" !! json"""{"ts":1415709559,"ts":1415700000}""" !
        json"""{ "ts": "2014-11-11T12:39:19.000Z", "ts": "2014-11-11T10:00:00.000Z"}""" |
      "Change nested values" !! json"""{"ts":1415709559,"nested":{"ts":1415700000}}""" !
        json"""{ "ts": "2014-11-11T12:39:19.000Z", "nested": {"ts": "2014-11-11T10:00:00.000Z" }}""" |
      "Change nested string values" !! json"""{"ts":1415709559,"nested":{"ts":"1415700000"}}""" !
        json"""{ "ts": "2014-11-11T12:39:19.000Z", "nested": { "ts": "2014-11-11T10:00:00.000Z" }}""" |
      "JStrings should also be changed" !! json"""{"ts":"1415709559"}""" !
        json"""{ "ts" : "2014-11-11T12:39:19.000Z" }""" |> { (_, json, expected) =>
      BaseAdapter.cleanupJsonEventValues(json, None, List("ts")) mustEqual expected
    }

  def e10 =
    "SPEC NAME" || "JSON" | "EXPECTED OUTPUT" |
      "Remove 'event'->'type'" !! json"""{"an_event":"type"}""" ! json"""{}""" |
      "Not remove existing values" !! json"""{"abc":1415709559, "an_event":"type", "cba":"type"}""" !
        json"""{ "abc": 1415709559, "cba": "type" }""" |
      "Works with ts value subs" !! json"""{"ts":1415709559, "an_event":"type", "abc":"type"}""" !
        json"""{ "ts": "2014-11-11T12:39:19.000Z", "abc": "type" }""" |
      "Removes nested values" !! json"""{"abc":"abc","nested":{"an_event":"type"}}""" !
        json"""{ "abc": "abc", "nested": {}}""" |> { (_, json, expected) =>
      BaseAdapter.cleanupJsonEventValues(json, ("an_event", "type").some, List("ts")) mustEqual
        expected
    }

}
