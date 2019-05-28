/*
 * Copyright (c) 2016-2019 Snowplow Analytics Ltd. All rights reserved.
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
import org.joda.time.DateTime
import org.specs2.Specification
import org.specs2.matcher.{DataTables, ValidatedMatchers}

import loaders._
import outputs._
import utils.Clock._

class UnbounceAdapterSpec extends Specification with DataTables with ValidatedMatchers {
  def is = s2"""
    This is a specification to test the UnbounceAdapter functionality
    toRawEvents must return a Success Nel if the query string is valid                            $e1
    toRawEvents must return a Nel Failure if the request body is missing                          $e2
    toRawEvents must return a Nel Failure if the content type is missing                          $e3
    toRawEvents must return a Nel Failure if the content type is incorrect                        $e4
    toRawEvents must return a Failure Nel if the event body is empty                              $e5
    payloadBodyToEvent must return a Failure if the event data does not have 'data.json' as a key $e6
    payloadBodyToEvent must return a Failure if the event data is empty                           $e7
    payloadBodyToEvent must return a Failure if the event string failed to parse into JSON        $e8
    payloadBodyToContext must return a Failure if the context data is missing 'page_id'           $e9
    payloadBodyToContext must return a Failure if the context data is missing 'page_name'         $e10
    payloadBodyToContext must return a Failure if the context data is missing 'variant'           $e11
    payloadBodyToContext must return a Failure if the context data is missing 'page_url'          $e12
    """

  object Shared {
    val api = CollectorApi("com.unbounce", "v1")
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

  val ContentType = "application/x-www-form-urlencoded"

  def e1 = {
    val params = SpecHelpers.toNameValuePairs("schema" -> "iglu:com.unbounce/test/jsonschema/1-0-0")
    val body =
      "page_url=http%3A%2F%2Funbouncepages.com%2Fwayfaring-147%2F&page_name=Wayfaring&page_id=7648177d-7323-4330-b4f9-9951a52138b6&variant=a&data.json=%7B%22userfield1%22%3A%5B%22asdfasdf%22%5D%2C%22ip_address%22%3A%5B%2285.73.39.163%22%5D%2C%22page_uuid%22%3A%5B%227648177d-7323-4330-b4f9-9951a52138b6%22%5D%2C%22variant%22%3A%5B%22a%22%5D%2C%22time_submitted%22%3A%5B%2211%3A45+AM+UTC%22%5D%2C%22date_submitted%22%3A%5B%222017-11-15%22%5D%2C%22page_url%22%3A%5B%22http%3A%2F%2Funbouncepages.com%2Fwayfaring-147%2F%22%5D%2C%22page_name%22%3A%5B%22Wayfaring%22%5D%7D&data.xml=%3C%3Fxml+version%3D%221.0%22+encoding%3D%22UTF-8%22%3F%3E%3Cform_data%3E%3Cuserfield1%3Easdfasdf%3C%2Fuserfield1%3E%3Cip_address%3E85.73.39.163%3C%2Fip_address%3E%3Cpage_uuid%3E7648177d-7323-4330-b4f9-9951a52138b6%3C%2Fpage_uuid%3E%3Cvariant%3Ea%3C%2Fvariant%3E%3Ctime_submitted%3E11%3A45+AM+UTC%3C%2Ftime_submitted%3E%3Cdate_submitted%3E2017-11-15%3C%2Fdate_submitted%3E%3Cpage_url%3Ehttp%3A%2F%2Funbouncepages.com%2Fwayfaring-147%2F%3C%2Fpage_url%3E%3Cpage_name%3EWayfaring%3C%2Fpage_name%3E%3C%2Fform_data%3E"
    val payload = CollectorPayload(
      Shared.api,
      params,
      ContentType.some,
      body.some,
      Shared.cljSource,
      Shared.context
    )
    val expectedJson =
      """{
        |"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0",
        |"data":{
          |"schema":"iglu:com.unbounce/form_post/jsonschema/1-0-0",
          |"data":{
            |"data.json":{
              |"userfield1":[
                |"asdfasdf"
              |],
              |"ipAddress":[
                |"85.73.39.163"
              |],
              |"pageUuid":[
                |"7648177d-7323-4330-b4f9-9951a52138b6"
              |],
              |"variant":[
                |"a"
              |],
              |"timeSubmitted":[
                |"11:45 AM UTC"
              |],
              |"dateSubmitted":[
                |"2017-11-15"
              |],
              |"pageUrl":[
                |"http://unbouncepages.com/wayfaring-147/"
              |],
              |"pageName":[
                |"Wayfaring"
              |]
            |},
            |"variant":"a",
            |"pageId":"7648177d-7323-4330-b4f9-9951a52138b6",
            |"pageName":"Wayfaring",
            |"pageUrl":"http://unbouncepages.com/wayfaring-147/"
          |}
        |}
      |}""".stripMargin.replaceAll("[\n\r]", "")
    val expected = NonEmptyList.one(
      RawEvent(
        Shared.api,
        Map("tv" -> "com.unbounce-v1", "e" -> "ue", "p" -> "srv", "ue_pr" -> expectedJson),
        ContentType.some,
        Shared.cljSource,
        Shared.context
      )
    )
    UnbounceAdapter.toRawEvents(payload, SpecHelpers.client).value must beValid(expected)
  }

  def e2 = {
    val params = SpecHelpers.toNameValuePairs("schema" -> "iglu:com.unbounce/test/jsonschema/1-0-0")
    val payload =
      CollectorPayload(Shared.api, params, ContentType.some, None, Shared.cljSource, Shared.context)
    UnbounceAdapter.toRawEvents(payload, SpecHelpers.client).value must beInvalid(
      NonEmptyList.one(InputDataAdapterFailure("body", None, "empty body: no events to process"))
    )
  }

  def e3 = {
    val body =
      "page_id=f7afd389-65a3-45fa-8bad-b7a42236044c&page_name=Test-Webhook&variant=a&page_url=http%3A%2F%2Funbouncepages.com%2Ftest-webhook-1&data.json=%7B%22email%22%3A%5B%22test%40snowplowanalytics.com%22%5D%2C%22ip_address%22%3A%5B%22200.121.220.179%22%5D%2C%22time_submitted%22%3A%5B%2204%3A17%20PM%20UTC%22%5D%7D"
    val payload =
      CollectorPayload(Shared.api, Nil, None, body.some, Shared.cljSource, Shared.context)
    UnbounceAdapter.toRawEvents(payload, SpecHelpers.client).value must beInvalid(
      NonEmptyList.one(
        InputDataAdapterFailure(
          "contentType",
          None,
          "no content type: expected application/x-www-form-urlencoded"
        )
      )
    )
  }

  def e4 = {
    val body =
      "page_id=f7afd389-65a3-45fa-8bad-b7a42236044c&page_name=Test-Webhook&variant=a&page_url=http%3A%2F%2Funbouncepages.com%2Ftest-webhook-1&data.json=%7B%22email%22%3A%5B%22test%40snowplowanalytics.com%22%5D%2C%22ip_address%22%3A%5B%22200.121.220.179%22%5D%2C%22time_submitted%22%3A%5B%2204%3A17%20PM%20UTC%22%5D%7D"
    val ct = "application/json"
    val payload =
      CollectorPayload(Shared.api, Nil, ct.some, body.some, Shared.cljSource, Shared.context)
    UnbounceAdapter.toRawEvents(payload, SpecHelpers.client).value must beInvalid(
      NonEmptyList.one(
        InputDataAdapterFailure(
          "contentType",
          "application/json".some,
          "expected application/x-www-form-urlencoded"
        )
      )
    )
  }

  def e5 = {
    val params = SpecHelpers.toNameValuePairs("schema" -> "iglu:com.unbounce/test/jsonschema/1-0-0")
    val body = ""
    val payload = CollectorPayload(
      Shared.api,
      params,
      ContentType.some,
      body.some,
      Shared.cljSource,
      Shared.context
    )
    val expected =
      NonEmptyList.one(InputDataAdapterFailure("body", None, "empty body: no events to process"))
    UnbounceAdapter.toRawEvents(payload, SpecHelpers.client).value must beInvalid(expected)
  }

  def e6 = {
    val params = SpecHelpers.toNameValuePairs("schema" -> "iglu:com.unbounce/test/jsonschema/1-0-0")
    val body =
      "page_id=f7afd389-65a3-45fa-8bad-b7a42236044c&page_name=Test-Webhook&variant=a&page_url=http%3A%2F%2Funbouncepages.com%2Ftest-webhook-1"
    val payload = CollectorPayload(
      Shared.api,
      params,
      ContentType.some,
      body.some,
      Shared.cljSource,
      Shared.context
    )
    val expected = NonEmptyList.one(
      InputDataAdapterFailure("data.json", None, "missing 'data.json' field in body")
    )
    UnbounceAdapter.toRawEvents(payload, SpecHelpers.client).value must beInvalid(expected)
  }

  def e7 = {
    val params = SpecHelpers.toNameValuePairs("schema" -> "iglu:com.unbounce/test/jsonschema/1-0-0")
    val body =
      "page_id=f7afd389-65a3-45fa-8bad-b7a42236044c&page_name=Test-Webhook&variant=a&page_url=http%3A%2F%2Funbouncepages.com%2Ftest-webhook-1&data.json="
    val payload = CollectorPayload(
      Shared.api,
      params,
      ContentType.some,
      body.some,
      Shared.cljSource,
      Shared.context
    )
    val expected = NonEmptyList.one(
      InputDataAdapterFailure("data.json", None, "empty 'data.json' field in body")
    )
    UnbounceAdapter.toRawEvents(payload, SpecHelpers.client).value must beInvalid(expected)
  }

  def e8 = {
    val params = SpecHelpers.toNameValuePairs("schema" -> "iglu:com.unbounce/test/jsonschema/1-0-0")
    val body =
      "page_id=f7afd389-65a3-45fa-8bad-b7a42236044c&page_name=Test-Webhook&variant=a&page_url=http%3A%2F%2Funbouncepages.com%2Ftest-webhook-1&data.json=%7B%7B%22email%22%3A%5B%22test%40snowplowanalytics.com%22%5D%2C%22ip_address%22%3A%5B%22200.121.220.179%22%5D%2C%22time_submitted%22%3A%5B%2204%3A17%20PM%20UTC%22%5D%7D"
    val payload = CollectorPayload(
      Shared.api,
      params,
      ContentType.some,
      body.some,
      Shared.cljSource,
      Shared.context
    )
    val expected = NonEmptyList.one(
      NotJsonAdapterFailure(
        "data.json",
        """{{"email":["test@snowplowanalytics.com"],"ip_address":["200.121.220.179"],"time_submitted":["04:17 PM UTC"]}""".some,
        """invalid json: expected " got '{"emai...' (line 1, column 2)"""
      )
    )
    UnbounceAdapter.toRawEvents(payload, SpecHelpers.client).value must beInvalid(expected)
  }

  def e9 = {
    val params = SpecHelpers.toNameValuePairs("schema" -> "iglu:com.unbounce/test/jsonschema/1-0-0")
    val body =
      "page_name=Test-Webhook&variant=a&page_url=http%3A%2F%2Funbouncepages.com%2Ftest-webhook-1&data.json=%7B%22email%22%3A%5B%22test%40snowplowanalytics.com%22%5D%2C%22ip_address%22%3A%5B%22200.121.220.179%22%5D%2C%22time_submitted%22%3A%5B%2204%3A17%20PM%20UTC%22%5D%7D"
    val payload = CollectorPayload(
      Shared.api,
      params,
      ContentType.some,
      body.some,
      Shared.cljSource,
      Shared.context
    )
    val expected =
      NonEmptyList.one(InputDataAdapterFailure("page_id", None, "missing 'page_id' field in body"))
    UnbounceAdapter.toRawEvents(payload, SpecHelpers.client).value must beInvalid(expected)
  }

  def e10 = {
    val params = SpecHelpers.toNameValuePairs("schema" -> "iglu:com.unbounce/test/jsonschema/1-0-0")
    val body =
      "page_id=f7afd389-65a3-45fa-8bad-b7a42236044c&variant=a&page_url=http%3A%2F%2Funbouncepages.com%2Ftest-webhook-1&data.json=%7B%22email%22%3A%5B%22test%40snowplowanalytics.com%22%5D%2C%22ip_address%22%3A%5B%22200.121.220.179%22%5D%2C%22time_submitted%22%3A%5B%2204%3A17%20PM%20UTC%22%5D%7D"
    val payload = CollectorPayload(
      Shared.api,
      params,
      ContentType.some,
      body.some,
      Shared.cljSource,
      Shared.context
    )
    val expected = NonEmptyList.one(
      InputDataAdapterFailure("page_name", None, "missing 'page_name' field in body")
    )
    UnbounceAdapter.toRawEvents(payload, SpecHelpers.client).value must beInvalid(expected)
  }

  def e11 = {
    val params = SpecHelpers.toNameValuePairs("schema" -> "iglu:com.unbounce/test/jsonschema/1-0-0")
    val body =
      "page_id=f7afd389-65a3-45fa-8bad-b7a42236044c&page_name=Test-Webhook&page_url=http%3A%2F%2Funbouncepages.com%2Ftest-webhook-1&data.json=%7B%22email%22%3A%5B%22test%40snowplowanalytics.com%22%5D%2C%22ip_address%22%3A%5B%22200.121.220.179%22%5D%2C%22time_submitted%22%3A%5B%2204%3A17%20PM%20UTC%22%5D%7D"
    val payload = CollectorPayload(
      Shared.api,
      params,
      ContentType.some,
      body.some,
      Shared.cljSource,
      Shared.context
    )
    val expected =
      NonEmptyList.one(InputDataAdapterFailure("variant", None, "missing 'variant' field in body"))
    UnbounceAdapter.toRawEvents(payload, SpecHelpers.client).value must beInvalid(expected)
  }

  def e12 = {
    val params = SpecHelpers.toNameValuePairs("schema" -> "iglu:com.unbounce/test/jsonschema/1-0-0")
    val body =
      "page_id=f7afd389-65a3-45fa-8bad-b7a42236044c&page_name=Test-Webhook&variant=a&data.json=%7B%22email%22%3A%5B%22test%40snowplowanalytics.com%22%5D%2C%22ip_address%22%3A%5B%22200.121.220.179%22%5D%2C%22time_submitted%22%3A%5B%2204%3A17%20PM%20UTC%22%5D%7D"
    val payload = CollectorPayload(
      Shared.api,
      params,
      ContentType.some,
      body.some,
      Shared.cljSource,
      Shared.context
    )
    val expected = NonEmptyList.one(
      InputDataAdapterFailure("page_url", None, "missing 'page_url' field in body")
    )
    UnbounceAdapter.toRawEvents(payload, SpecHelpers.client).value must beInvalid(expected)
  }
}
