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

class SnowplowAdapterSpec extends Specification with DataTables with ValidationMatchers with ScalaCheck { def is =

  "This is a specification to test the SnowplowAdapter functionality"                                                       ^
                                                                                                                           p^
  "Tp1.toRawEvents should return a NEL containing one RawEvent if the querystring is populated"                             ! e1^
  "Tp1.toRawEvents should return a Validation Failure if the querystring is empty"                                          ! e2^
  "Tp2.toRawEvents should return a NEL containing one RawEvent if only the querystring is populated"                        ! e3^
  "Tp2.toRawEvents should return a NEL containing one RawEvent if the querystring is empty but the body contains one event" ! e4^
  "Tp2.toRawEvents should return a NEL containing three RawEvents consolidating body's events and querystring's parameters" ! e5^
  "Tp1.toRawEvents should return a NEL containing one RawEvent if the Content-Type is application/json; charset=UTF-8"      ! e6^
  "Tp2.toRawEvents should return a Validation Failure if querystring, body and content type are mismatching"                ! e8^
  "Tp2.toRawEvents should return a Validation Failure if the body is not a self-describing JSON"                            ! e8^
  "Tp2.toRawEvents should return a Validation Failure if the body is in a JSON Schema other than payload_data"              ! e9^
  "Tp2.toRawEvents should return a Validation Failure if the body fails payload_data JSON Schema validation"                ! e10^
                                                                                                                            end

  implicit val resolver = SpecHelpers.IgluResolver

  object Snowplow {
    private val api: (String) => CollectorApi = version => CollectorApi("com.snowplowanalytics.snowplow", version)
    val Tp1 = api("tp1")
    val Tp2 = api("tp2")
  }

  val ApplicationJson = "application/json"
  val ApplicationJsonWithCharset = "application/json; charset=utf-8"
  val ApplicationJsonWithCapitalCharset = "application/json; charset=UTF-8"

  object Shared {
    val source = CollectorSource("clj-tomcat", "UTF-8", None)
    val context = CollectorContext(DateTime.parse("2013-08-29T00:18:48.000+00:00").some, "37.157.33.123".some, None, None, Nil, None)
  }

  def e1 = {
    val payload = CollectorPayload(Snowplow.Tp1, toNameValuePairs("aid" -> "test"), None, None, Shared.source, Shared.context)
    val actual = SnowplowAdapter.Tp1.toRawEvents(payload)
    actual must beSuccessful(NonEmptyList(RawEvent(Snowplow.Tp1, Map("aid" -> "test"), None, Shared.source, Shared.context)))
  }

  def e2 = {
    val payload = CollectorPayload(Snowplow.Tp1, Nil, None, None, Shared.source, Shared.context)
    val actual = SnowplowAdapter.Tp1.toRawEvents(payload)
    actual must beFailing(NonEmptyList("Querystring is empty: no raw event to process"))
  }

  def e3 = {
    val payload = CollectorPayload(Snowplow.Tp2, toNameValuePairs("aid" -> "tp2", "e" -> "se"), None, None, Shared.source, Shared.context)
    val actual = SnowplowAdapter.Tp2.toRawEvents(payload)
    actual must beSuccessful(NonEmptyList(RawEvent(Snowplow.Tp2, Map("aid" -> "tp2", "e" -> "se"), None, Shared.source, Shared.context)))
  }

  def e4 = {
    val body = toSelfDescJson("""[{"tv":"ios-0.1.0","p":"mob","e":"se"}]""", "payload_data")
    val payload = CollectorPayload(Snowplow.Tp2, Nil, ApplicationJsonWithCharset.some, body.some, Shared.source, Shared.context)
    val actual = SnowplowAdapter.Tp2.toRawEvents(payload)
    actual must beSuccessful(NonEmptyList(RawEvent(Snowplow.Tp2, Map("tv" -> "ios-0.1.0", "p" -> "mob", "e" -> "se"), ApplicationJsonWithCharset.some, Shared.source, Shared.context)))
  }

  def e5 = {
    val body = toSelfDescJson("""[{"tv":"1","p":"1","e":"1"},{"tv":"2","p":"2","e":"2"},{"tv":"3","p":"3","e":"3"}]""", "payload_data")
    val payload = CollectorPayload(Snowplow.Tp2, toNameValuePairs("tv" -> "0", "nuid" -> "123"), ApplicationJsonWithCapitalCharset.some, body.some, Shared.source, Shared.context)
    val actual = SnowplowAdapter.Tp2.toRawEvents(payload)

    val rawEvent: RawEventParameters => RawEvent = params => RawEvent(Snowplow.Tp2, params, ApplicationJsonWithCapitalCharset.some, Shared.source, Shared.context)
    actual must beSuccessful(NonEmptyList(
      rawEvent(Map("tv" -> "0", "p" -> "1", "e" -> "1", "nuid" -> "123")),
      rawEvent(Map("tv" -> "0", "p" -> "2", "e" -> "2", "nuid" -> "123")),
      rawEvent(Map("tv" -> "0", "p" -> "3", "e" -> "3", "nuid" -> "123"))
    ))
  }

  def e6 = {
    val body = toSelfDescJson("""[{"tv":"ios-0.1.0","p":"mob","e":"se"}]""", "payload_data")
    val payload = CollectorPayload(Snowplow.Tp2, Nil, ApplicationJsonWithCapitalCharset.some, body.some, Shared.source, Shared.context)
    val actual = SnowplowAdapter.Tp2.toRawEvents(payload)
    actual must beSuccessful(NonEmptyList(RawEvent(Snowplow.Tp2, Map("tv" -> "ios-0.1.0", "p" -> "mob", "e" -> "se"), ApplicationJsonWithCapitalCharset.some, Shared.source, Shared.context)))
  }

  def e7 =
    "SPEC NAME"                               || "IN QUERYSTRING"             | "IN CONTENT TYPE"    | "IN BODY"   | "EXP. FAILURE"                                                                                            |
    "Invalid content type"                    !! Nil                          ! "text/plain".some    ! "body".some ! "Content type of text/plain provided, expected one of: application/json, application/json; charset=utf-8" |
    "Neither querystring nor body populated"  !! Nil                          ! None                 ! None        ! "Request body and querystring parameters empty, expected at least one populated"                          |
    "Body populated but content type missing" !! Nil                          ! None                 ! "body".some ! "Request body provided but content type empty, expected one of: application/json, application/json; charset=utf-8" |
    "Content type populated but body missing" !! toNameValuePairs("a" -> "b") ! ApplicationJsonWithCharset.some ! None        ! "Content type of application/json; charset=utf-8 provided but request body empty"                         |
    "Body is not a JSON"                      !! toNameValuePairs("a" -> "b") ! ApplicationJson.some ! "body".some ! "Field [Body]: invalid JSON [body] with parsing error: Unrecognized token 'body': was expecting ('true', 'false' or 'null') at [Source: java.io.StringReader@xxxxxx; line: 1, column: 9]" |> {

      (_, querystring, contentType, body, expected) => {

        val payload = CollectorPayload(Snowplow.Tp2, querystring, contentType, body, Shared.source, Shared.context)
        val actual = SnowplowAdapter.Tp2.toRawEvents(payload)
        actual must beFailing(NonEmptyList(expected))
      }
    }

  def e8 = {
    val payload = CollectorPayload(Snowplow.Tp2, toNameValuePairs("aid" -> "test"), ApplicationJson.some, """{"not":"self-desc"}""".some, Shared.source, Shared.context)
    val actual = SnowplowAdapter.Tp2.toRawEvents(payload)
    actual must beFailing(NonEmptyList("""error: object instance has properties which are not allowed by the schema: ["not"]
    level: "error"
    schema: {"loadingURI":"#","pointer":""}
    instance: {"pointer":""}
    domain: "validation"
    keyword: "additionalProperties"
    unwanted: ["not"]
""",
    """error: object has missing required properties (["data","schema"])
    level: "error"
    schema: {"loadingURI":"#","pointer":""}
    instance: {"pointer":""}
    domain: "validation"
    keyword: "required"
    required: ["data","schema"]
    missing: ["data","schema"]
"""))
  }

  def e9 = {
    val body = toSelfDescJson("""{"longitude":20.1234}""", "geolocation_context")
    val payload = CollectorPayload(Snowplow.Tp2, Nil, ApplicationJson.some, body.some, Shared.source, Shared.context)
    val actual = SnowplowAdapter.Tp2.toRawEvents(payload)
    actual must beFailing(NonEmptyList("""error: Verifying schema as iglu:com.snowplowanalytics.snowplow/payload_data/jsonschema/1-0-* failed: found iglu:com.snowplowanalytics.snowplow/geolocation_context/jsonschema/1-0-0
    level: "error"
"""))
  }

  def e10 =
    "SPEC NAME"                    || "IN JSON DATA"                                              | "EXP. FAILURES" |
    "JSON object instead of array" !! "{}"                                                        ! NonEmptyList("""error: instance type (object) does not match any allowed primitive type (allowed: ["array"])
    level: "error"
    schema: {"loadingURI":"#","pointer":""}
    instance: {"pointer":""}
    domain: "validation"
    keyword: "type"
    found: "object"
    expected: ["array"]
""") |
    "Missing required properties"  !! """[{"tv":"ios-0.1.0"}]"""                                  ! NonEmptyList("""error: object has missing required properties (["e","p"])
    level: "error"
    schema: {"loadingURI":"#","pointer":"/items"}
    instance: {"pointer":"/0"}
    domain: "validation"
    keyword: "required"
    required: ["e","p","tv"]
    missing: ["e","p"]
""") |
    "1 valid, 1 invalid"           !! """[{"tv":"ios-0.1.0","p":"mob","e":"se"},{"new":"foo"}]""" ! NonEmptyList("""error: object instance has properties which are not allowed by the schema: ["new"]
    level: "error"
    schema: {"loadingURI":"#","pointer":"/items"}
    instance: {"pointer":"/1"}
    domain: "validation"
    keyword: "additionalProperties"
    unwanted: ["new"]
""",
    """error: object has missing required properties (["e","p","tv"])
    level: "error"
    schema: {"loadingURI":"#","pointer":"/items"}
    instance: {"pointer":"/1"}
    domain: "validation"
    keyword: "required"
    required: ["e","p","tv"]
    missing: ["e","p","tv"]
""") |> {

      (_, json, expected) => {

        val body = toSelfDescJson(json, "payload_data")
        val payload = CollectorPayload(Snowplow.Tp2, Nil, ApplicationJson.some, body.some, Shared.source, Shared.context)

        val actual = SnowplowAdapter.Tp2.toRawEvents(payload)
        actual must beFailing(expected)
      }
    }

}
