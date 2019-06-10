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

import cats.data.NonEmptyList
import cats.syntax.option._
import com.snowplowanalytics.snowplow.badrows.AdapterFailure._
import com.snowplowanalytics.snowplow.badrows.Payload.{CollectorPayload => _}
import org.joda.time.DateTime
import org.specs2.Specification
import org.specs2.matcher.{DataTables, ValidatedMatchers}

import loaders._
import utils.Clock._

class IgluAdapterSpec extends Specification with DataTables with ValidatedMatchers {
  def is = s2"""
  This is a specification to test the IgluAdapter functionality
  toRawEvents should return a NEL containing one RawEvent if the CloudFront querystring is minimally populated           $e1
  toRawEvents should return a NEL containing one RawEvent if the CloudFront querystring is maximally populated           $e2
  toRawEvents should return a NEL containing one RawEvent if the Clojure-Tomcat querystring is populated                 $e3
  toRawEvents should return a NEL containing one RawEvent with a non-app platform if specified                           $e4
  toRawEvents should return a Validation Failure if there are no parameters on the CloudFront querystring                $e5
  toRawEvents should return a Validation Failure if there is no schema parameter on the CloudFront querystring           $e6
  toRawEvents should return a Validation Failure if the schema parameter is not in an Iglu-compatible format             $e7
  toRawEvents should return a NEL containing one RawEvent if the schema parameter is populated with a valid content type $e8
  toRawEvents should return a Validation Failure if an unsupported content type is specified                             $e9
  toRawEvents should return a Validation Failure if there are no events in the JSON payload                              $e10
  toRawEvents should return a Validation Failure if body is specified but a content type is not provided                 $e11
  toRawEvents should return a NEL containing one RawEvent if a sd-json is found                                          $e12
  toRawEvents should return a Validation Failure if sd-json is found but content type is not supported                   $e13
  toRawEvents should return a Validation Failure if body is found but content type is not supported                      $e14
  toRawEvents should return a NEL containing one RawEvent if a querystring is found in the POST body                     $e15
  toRawEvents should return a NEL with RawEvents if the schema is in the qs and the body contains an array               $e16
  toRawEvents should return a Validation Failure if the schema is in the qs and the body contains an empty array         $e17
  """

  object Shared {
    val api = CollectorApi("com.snowplowanalytics.iglu", "v1")
    val cfSource = CollectorSource("cloudfront", "UTF-8", None)
    val cljSource = CollectorSource("clj-tomcat", "UTF-8", None)
    val context = CollectorContext(
      DateTime.parse("2013-08-29T00:18:48.000+00:00").some,
      "37.157.33.123".some,
      None,
      None,
      Nil,
      None
    )
  }

  object Expected {
    val staticNoPlatform = Map(
      "tv" -> "com.snowplowanalytics.iglu-v1",
      "e" -> "ue"
    )
    val static = staticNoPlatform ++ Map(
      "p" -> "app"
    )
  }

  def e1 = {
    val params = SpecHelpers.toNameValuePairs(
      "schema" -> "iglu:com.acme/campaign/jsonschema/1-0-0",
      "user" -> "6353af9b-e288-4cf3-9f1c-b377a9c84dac",
      "name" -> "download",
      "publisher_name" -> "Organic",
      "source" -> "",
      "tracking_id" -> "",
      "ad_unit" -> ""
    )
    val payload = CollectorPayload(Shared.api, params, None, None, Shared.cfSource, Shared.context)
    val actual = IgluAdapter.toRawEvents(payload, SpecHelpers.client).value

    val expectedJson =
      """|{
            |"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0",
            |"data":{
              |"schema":"iglu:com.acme/campaign/jsonschema/1-0-0",
              |"data":{
                |"name":"download",
                |"source":null,
                |"ad_unit":null,
                |"tracking_id":null,
                |"publisher_name":"Organic",
                |"user":"6353af9b-e288-4cf3-9f1c-b377a9c84dac"
              |}
            |}
          |}""".stripMargin.replaceAll("[\n\r]", "")

    actual must beValid(
      NonEmptyList.one(
        RawEvent(
          Shared.api,
          Expected.static ++ Map("ue_pr" -> expectedJson),
          None,
          Shared.cfSource,
          Shared.context
        )
      )
    )
  }

  def e2 = {
    val params = SpecHelpers.toNameValuePairs(
      "schema" -> "iglu:com.acme/campaign/jsonschema/1-0-0",
      "user" -> "6353af9b-e288-4cf3-9f1c-b377a9c84dac",
      "name" -> "install",
      "source" -> "newsfeed",
      "tracking_id" -> "3353af9c-e298-2cf3-9f1c-b377a9c84dad",
      "ad_unit" -> "UN-11-b",
      "aid" -> "webhooks"
    )
    val payload = CollectorPayload(Shared.api, params, None, None, Shared.cfSource, Shared.context)
    val actual = IgluAdapter.toRawEvents(payload, SpecHelpers.client).value

    val expectedMap = {
      val json =
        """|{
              |"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0",
              |"data":{
                |"schema":"iglu:com.acme/campaign/jsonschema/1-0-0",
                |"data":{
                  |"name":"install",
                  |"source":"newsfeed",
                  |"ad_unit":"UN-11-b",
                  |"tracking_id":"3353af9c-e298-2cf3-9f1c-b377a9c84dad",
                  |"user":"6353af9b-e288-4cf3-9f1c-b377a9c84dac"
                |}
              |}
            |}""".stripMargin.replaceAll("[\n\r]", "")
      Map(
        "ue_pr" -> json,
        "aid" -> "webhooks"
      )
    }

    actual must beValid(
      NonEmptyList.one(
        RawEvent(Shared.api, Expected.static ++ expectedMap, None, Shared.cfSource, Shared.context)
      )
    )
  }

  def e3 = {
    val params = SpecHelpers.toNameValuePairs(
      "schema" -> "iglu:com.acme/campaign/jsonschema/2-0-0",
      "user" -> "6353af9b-e288-4cf3-9f1c-b377a9c84dac",
      "name" -> "retarget",
      "source" -> "newsfeed",
      "tracking_id" -> "",
      "ad_unit" -> "UN-11-b",
      "aid" -> "my webhook project",
      "cv" -> "clj-0.5.0-tom-0.0.4",
      "nuid" -> ""
    )
    val payload = CollectorPayload(Shared.api, params, None, None, Shared.cljSource, Shared.context)
    val actual = IgluAdapter.toRawEvents(payload, SpecHelpers.client).value

    val expectedMap = {
      val json =
        """|{
              |"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0",
              |"data":{
                |"schema":"iglu:com.acme/campaign/jsonschema/2-0-0",
                |"data":{
                  |"name":"retarget",
                  |"source":"newsfeed",
                  |"ad_unit":"UN-11-b",
                  |"tracking_id":null,
                  |"user":"6353af9b-e288-4cf3-9f1c-b377a9c84dac"
                |}
              |}
            |}""".stripMargin.replaceAll("[\n\r]", "")
      Map(
        "ue_pr" -> json,
        "aid" -> "my webhook project",
        "cv" -> "clj-0.5.0-tom-0.0.4",
        "nuid" -> ""
      )
    }

    actual must beValid(
      NonEmptyList.one(
        RawEvent(Shared.api, Expected.static ++ expectedMap, None, Shared.cljSource, Shared.context)
      )
    )
  }

  def e4 = {
    val params = SpecHelpers.toNameValuePairs(
      "schema" -> "iglu:com.acme/campaign/jsonschema/1-0-1",
      "user" -> "6353af9b-e288-4cf3-9f1c-b377a9c84dac",
      "name" -> "download",
      "p" -> "mob"
    )
    val payload = CollectorPayload(Shared.api, params, None, None, Shared.cfSource, Shared.context)
    val actual = IgluAdapter.toRawEvents(payload, SpecHelpers.client).value

    val expectedJson =
      """|{
            |"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0",
            |"data":{
              |"schema":"iglu:com.acme/campaign/jsonschema/1-0-1",
              |"data":{
                |"user":"6353af9b-e288-4cf3-9f1c-b377a9c84dac",
                |"name":"download"
              |}
            |}
          |}""".stripMargin.replaceAll("[\n\r]", "")

    actual must beValid(
      NonEmptyList.one(
        RawEvent(
          Shared.api,
          Expected.staticNoPlatform ++ Map("p" -> "mob", "ue_pr" -> expectedJson),
          None,
          Shared.cfSource,
          Shared.context
        )
      )
    )
  }

  def e5 = {
    val params = SpecHelpers.toNameValuePairs()
    val payload = CollectorPayload(Shared.api, params, None, None, Shared.cfSource, Shared.context)
    val actual = IgluAdapter.toRawEvents(payload, SpecHelpers.client).value

    actual must beInvalid(
      NonEmptyList.of(
        InputDataAdapterFailure("schema", None, "empty `schema` field"),
        InputDataAdapterFailure("body", None, "empty body")
      )
    )
  }

  def e6 = {
    val params = SpecHelpers.toNameValuePairs(
      "some_param" -> "foo",
      "p" -> "mob"
    )
    val payload = CollectorPayload(Shared.api, params, None, None, Shared.cfSource, Shared.context)
    val actual = IgluAdapter.toRawEvents(payload, SpecHelpers.client).value

    actual must beInvalid(
      NonEmptyList.of(
        InputDataAdapterFailure("schema", None, "empty `schema` field"),
        InputDataAdapterFailure("body", None, "empty body")
      )
    )
  }

  def e7 = {
    val params = SpecHelpers.toNameValuePairs(
      "schema" -> "iglooooooo://blah"
    )
    val payload = CollectorPayload(Shared.api, params, None, None, Shared.cfSource, Shared.context)
    val actual = IgluAdapter.toRawEvents(payload, SpecHelpers.client).value

    actual must beInvalid(
      NonEmptyList
        .one(InputDataAdapterFailure("schema", "iglooooooo://blah".some, "INVALID_IGLUURI"))
    )
  }

  def e8 = {
    val params = SpecHelpers.toNameValuePairs(
      "schema" -> "iglu:com.acme/campaign/jsonschema/1-0-1",
      "some_param" -> "foo",
      "p" -> "mob"
    )
    val jsonStr = """{"key":"value", "everwets":"processed"}"""
    val payload =
      CollectorPayload(
        Shared.api,
        params,
        "application/json".some,
        jsonStr.some,
        Shared.cljSource,
        Shared.context
      )
    val actual = IgluAdapter.toRawEvents(payload, SpecHelpers.client).value

    val expected = RawEvent(
      Shared.api,
      Map(
        "tv" -> "com.snowplowanalytics.iglu-v1",
        "e" -> "ue",
        "p" -> "mob",
        "ue_pr" -> """{"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0","data":{"schema":"iglu:com.acme/campaign/jsonschema/1-0-1","data":{"key":"value","everwets":"processed"}}}"""
      ),
      "application/json".some,
      Shared.cljSource,
      Shared.context
    )

    actual must beValid(NonEmptyList.one(expected))
  }

  def e9 = {
    val params = SpecHelpers.toNameValuePairs(
      "schema" -> "iglu:com.acme/campaign/jsonschema/1-0-1",
      "some_param" -> "foo",
      "p" -> "mob"
    )
    val jsonStr = """{"key":"value", "everwets":"processed"}"""
    val payload =
      CollectorPayload(
        Shared.api,
        params,
        "application/badtype".some,
        jsonStr.some,
        Shared.cljSource,
        Shared.context
      )
    val actual = IgluAdapter.toRawEvents(payload, SpecHelpers.client).value

    val expected = InputDataAdapterFailure(
      "contentType",
      "application/badtype".some,
      "expected one of application/json, application/json; charset=utf-8, application/x-www-form-urlencoded"
    )
    actual must beInvalid(NonEmptyList.one(expected))
  }

  def e10 = {
    val params = SpecHelpers.toNameValuePairs(
      "schema" -> "iglu:com.acme/campaign/jsonschema/1-0-1",
      "some_param" -> "foo",
      "p" -> "mob"
    )
    val jsonStr = """{}"""
    val payload =
      CollectorPayload(
        Shared.api,
        params,
        "application/json".some,
        jsonStr.some,
        Shared.cljSource,
        Shared.context
      )
    val actual = IgluAdapter.toRawEvents(payload, SpecHelpers.client).value

    actual must beInvalid(
      NonEmptyList.one(InputDataAdapterFailure("body", "{}".some, "has no key-value pairs"))
    )
  }

  def e11 = {
    val params = SpecHelpers.toNameValuePairs(
      "schema" -> "iglu:com.acme/campaign/jsonschema/1-0-1",
      "some_param" -> "foo",
      "p" -> "mob"
    )
    val jsonStr = """{"key":"value"}"""
    val payload =
      CollectorPayload(Shared.api, params, None, jsonStr.some, Shared.cljSource, Shared.context)
    val actual = IgluAdapter.toRawEvents(payload, SpecHelpers.client).value

    actual must beInvalid(
      NonEmptyList.one(
        InputDataAdapterFailure(
          "contentType",
          None,
          "expected one of application/json, application/json; charset=utf-8, application/x-www-form-urlencoded"
        )
      )
    )
  }

  def e12 = {
    val params = SpecHelpers.toNameValuePairs("p" -> "mob")
    val jsonStr =
      """{"schema":"iglu:com.acme/campaign/jsonschema/1-0-1", "data":{"some_param":"foo"}}"""
    val payload =
      CollectorPayload(
        Shared.api,
        params,
        "application/json".some,
        jsonStr.some,
        Shared.cljSource,
        Shared.context
      )
    val actual = IgluAdapter.toRawEvents(payload, SpecHelpers.client).value

    val expected = RawEvent(
      Shared.api,
      Map(
        "tv" -> "com.snowplowanalytics.iglu-v1",
        "e" -> "ue",
        "p" -> "mob",
        "ue_pr" -> """{"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0","data":{"schema":"iglu:com.acme/campaign/jsonschema/1-0-1","data":{"some_param":"foo"}}}"""
      ),
      "application/json".some,
      Shared.cljSource,
      Shared.context
    )

    actual must beValid(NonEmptyList.one(expected))
  }

  def e13 = {
    val params = SpecHelpers.toNameValuePairs("p" -> "mob")
    val jsonStr =
      """{"schema":"iglu:com.acme/campaign/jsonschema/1-0-1", "data":{"some_param":"foo"}}"""
    val payload =
      CollectorPayload(
        Shared.api,
        params,
        "application/xxx-url-form-encoded".some,
        jsonStr.some,
        Shared.cljSource,
        Shared.context
      )
    val actual = IgluAdapter.toRawEvents(payload, SpecHelpers.client).value

    actual must beInvalid(
      NonEmptyList.one(
        InputDataAdapterFailure(
          "contentType",
          "application/xxx-url-form-encoded".some,
          "expected one of application/json, application/json; charset=utf-8"
        )
      )
    )
  }

  def e14 = {
    val params = SpecHelpers.toNameValuePairs("schema" -> "iglu:com.acme/campaign/jsonschema/1-0-1")
    val jsonStr = """{"some_param":"foo"}"""
    val payload =
      CollectorPayload(
        Shared.api,
        params,
        "application/xxx-url-form-encoded".some,
        jsonStr.some,
        Shared.cljSource,
        Shared.context
      )
    val actual = IgluAdapter.toRawEvents(payload, SpecHelpers.client).value

    actual must beInvalid(
      NonEmptyList.one(
        InputDataAdapterFailure(
          "contentType",
          "application/xxx-url-form-encoded".some,
          "expected one of application/json, application/json; charset=utf-8, application/x-www-form-urlencoded"
        )
      )
    )
  }

  def e15 = {
    val params = SpecHelpers.toNameValuePairs("schema" -> "iglu:com.acme/campaign/jsonschema/1-0-1")
    val formBodyStr = "some_param=foo&hello=world"
    val payload =
      CollectorPayload(
        Shared.api,
        params,
        "application/x-www-form-urlencoded".some,
        formBodyStr.some,
        Shared.cljSource,
        Shared.context
      )
    val actual = IgluAdapter.toRawEvents(payload, SpecHelpers.client).value

    val expected = RawEvent(
      Shared.api,
      Map(
        "tv" -> "com.snowplowanalytics.iglu-v1",
        "e" -> "ue",
        "p" -> "srv",
        "ue_pr" -> """{"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0","data":{"schema":"iglu:com.acme/campaign/jsonschema/1-0-1","data":{"some_param":"foo","hello":"world"}}}"""
      ),
      "application/x-www-form-urlencoded".some,
      Shared.cljSource,
      Shared.context
    )

    actual must beValid(NonEmptyList.one(expected))
  }

  def e16 = {
    val params = SpecHelpers.toNameValuePairs(
      "schema" -> "iglu:com.acme/campaign/jsonschema/1-0-1",
      "some_param" -> "foo",
      "p" -> "mob"
    )
    val jsonStr =
      """[{"key":"value", "everwets":"processed"},{"key":"value", "everwets":"processed"}]"""
    val payload =
      CollectorPayload(
        Shared.api,
        params,
        "application/json".some,
        jsonStr.some,
        Shared.cljSource,
        Shared.context
      )
    val actual = IgluAdapter.toRawEvents(payload, SpecHelpers.client).value

    val expected = RawEvent(
      Shared.api,
      Map(
        "tv" -> "com.snowplowanalytics.iglu-v1",
        "e" -> "ue",
        "p" -> "mob",
        "ue_pr" -> """{"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0","data":{"schema":"iglu:com.acme/campaign/jsonschema/1-0-1","data":{"key":"value","everwets":"processed"}}}"""
      ),
      "application/json".some,
      Shared.cljSource,
      Shared.context
    )

    actual must beValid(NonEmptyList.of(expected, expected))
  }

  def e17 = {
    val params = SpecHelpers.toNameValuePairs(
      "schema" -> "iglu:com.acme/campaign/jsonschema/1-0-1",
      "some_param" -> "foo",
      "p" -> "mob"
    )
    val jsonStr = """[]"""
    val payload =
      CollectorPayload(
        Shared.api,
        params,
        "application/json".some,
        jsonStr.some,
        Shared.cljSource,
        Shared.context
      )
    val actual = IgluAdapter.toRawEvents(payload, SpecHelpers.client).value

    actual must beInvalid(
      NonEmptyList.one(InputDataAdapterFailure("body", "[]".some, "empty array of events"))
    )
  }
}
