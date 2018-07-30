/*
 * Copyright (c) 2016-2018 Snowplow Analytics Ltd. All rights reserved.
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
import loaders.{CollectorApi, CollectorContext, CollectorPayload, CollectorSource}

import utils.ConversionUtils
import SpecHelpers._

// Specs2
import org.specs2.{ScalaCheck, Specification}
import org.specs2.matcher.DataTables
import org.specs2.scalaz.ValidationMatchers

class StatusGatorAdapterSpec extends Specification with DataTables with ValidationMatchers with ScalaCheck {
  def is = s2"""
    This is a specification to test the StatusgatorAdapter functionality
    toRawEvents must return a Success Nel if every event in the payload is successful          $e1
    toRawEvents must return a Nel Failure if the request body is missing                       $e2
    toRawEvents must return a Nel Failure if the content type is missing                       $e3
    toRawEvents must return a Nel Failure if the content type is incorrect                     $e4
    toRawEvents must return a Failure Nel if the event in the payload is incorrect             $e5
    toRawEvents must return a Failure String if the event string could not be parsed into JSON $e6
    """

  implicit val resolver = SpecHelpers.IgluResolver

  object Shared {
    val api       = CollectorApi("com.statusgator", "v1")
    val cljSource = CollectorSource("clj-tomcat", "UTF-8", None)
    val context = CollectorContext(DateTime.parse("2013-08-29T00:18:48.000+00:00").some,
                                   "37.157.33.123".some,
                                   None,
                                   None,
                                   Nil,
                                   None)
  }

  val ContentType = "application/x-www-form-urlencoded"

  def e1 = {
    val body =
      "service_name=CloudFlare&favicon_url=https%3A%2F%2Fdwxjd9cd6rwno.cloudfront.net%2Ffavicons%2Fcloudflare.ico&status_page_url=https%3A%2F%2Fwww.cloudflarestatus.com%2F&home_page_url=http%3A%2F%2Fwww.cloudflare.com&current_status=up&last_status=warn&occurred_at=2016-05-19T09%3A26%3A31%2B00%3A00"
    val payload = CollectorPayload(Shared.api, Nil, ContentType.some, body.some, Shared.cljSource, Shared.context)
    val expectedJson =
      """|{
          |"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0",
          |"data":{
            |"schema":"iglu:com.statusgator/status_change/jsonschema/1-0-0",
            |"data":{
              |"lastStatus":"warn",
              |"statusPageUrl":"https://www.cloudflarestatus.com/",
              |"serviceName":"CloudFlare",
              |"faviconUrl":"https://dwxjd9cd6rwno.cloudfront.net/favicons/cloudflare.ico",
              |"occurredAt":"2016-05-19T09:26:31+00:00",
              |"homePageUrl":"http://www.cloudflare.com",
              |"currentStatus":"up"
            |}
          |}
        |}""".stripMargin.replaceAll("[\n\r]", "")

    val expected = NonEmptyList(
      RawEvent(Shared.api,
               Map("tv" -> "com.statusgator-v1", "e" -> "ue", "p" -> "srv", "ue_pr" -> expectedJson),
               ContentType.some,
               Shared.cljSource,
               Shared.context))
    StatusGatorAdapter.toRawEvents(payload) must beSuccessful(expected)
  }

  def e2 = {
    val payload = CollectorPayload(Shared.api, Nil, ContentType.some, None, Shared.cljSource, Shared.context)
    StatusGatorAdapter.toRawEvents(payload) must beFailing(
      NonEmptyList("Request body is empty: no StatusGator events to process"))
  }

  def e3 = {
    val body =
      "service_name=CloudFlare&favicon_url=https%3A%2F%2Fdwxjd9cd6rwno.cloudfront.net%2Ffavicons%2Fcloudflare.ico&status_page_url=https%3A%2F%2Fwww.cloudflarestatus.com%2F&home_page_url=http%3A%2F%2Fwww.cloudflare.com&current_status=up&last_status=warn&occurred_at=2016-05-19T09%3A26%3A31%2B00%3A00"
    val payload = CollectorPayload(Shared.api, Nil, None, body.some, Shared.cljSource, Shared.context)
    StatusGatorAdapter.toRawEvents(payload) must beFailing(
      NonEmptyList(
        "Request body provided but content type empty, expected application/x-www-form-urlencoded for StatusGator"))
  }

  def e4 = {
    val body =
      "service_name=CloudFlare&favicon_url=https%3A%2F%2Fdwxjd9cd6rwno.cloudfront.net%2Ffavicons%2Fcloudflare.ico&status_page_url=https%3A%2F%2Fwww.cloudflarestatus.com%2F&home_page_url=http%3A%2F%2Fwww.cloudflare.com&current_status=up&last_status=warn&occurred_at=2016-05-19T09%3A26%3A31%2B00%3A00"
    val ct      = "application/json"
    val payload = CollectorPayload(Shared.api, Nil, ct.some, body.some, Shared.cljSource, Shared.context)
    StatusGatorAdapter.toRawEvents(payload) must beFailing(
      NonEmptyList(
        "Content type of application/json provided, expected application/x-www-form-urlencoded for StatusGator"))
  }

  def e5 = {
    val body     = ""
    val payload  = CollectorPayload(Shared.api, Nil, ContentType.some, body.some, Shared.cljSource, Shared.context)
    val expected = NonEmptyList("StatusGator event body is empty: nothing to process")
    StatusGatorAdapter.toRawEvents(payload) must beFailing(expected)
  }

  def e6 = {
    val body =
      "{service_name=CloudFlare&favicon_url=https%3A%2F%2Fdwxjd9cd6rwno.cloudfront.net%2Ffavicons%2Fcloudflare.ico&status_page_url=https%3A%2F%2Fwww.cloudflarestatus.com%2F&home_page_url=http%3A%2F%2Fwww.cloudflare.com&current_status=up&last_status=warn&occurred_at=2016-05-19T09%3A26%3A31%2B00%3A00"
    val payload = CollectorPayload(Shared.api, Nil, ContentType.some, body.some, Shared.cljSource, Shared.context)
    val expected = NonEmptyList(
      "StatusGator incorrect event string : [Illegal character in query at index 18: http://localhost/?{service_name=CloudFlare&favicon_url=https%3A%2F%2Fdwxjd9cd6rwno.cloudfront.net%2Ffavicons%2Fcloudflare.ico&status_page_url=https%3A%2F%2Fwww.cloudflarestatus.com%2F&home_page_url=http%3A%2F%2Fwww.cloudflare.com&current_status=up&last_status=warn&occurred_at=2016-05-19T09%3A26%3A31%2B00%3A00]")
    StatusGatorAdapter.toRawEvents(payload) must beFailing(expected)
  }
}
