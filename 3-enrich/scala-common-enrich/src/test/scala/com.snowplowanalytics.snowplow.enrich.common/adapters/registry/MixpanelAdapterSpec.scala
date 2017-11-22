/*
 * Copyright (c) 2016 Snowplow Analytics Ltd. All rights reserved.
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
  CollectorContext,
  CollectorSource,
  CollectorPayload
}

// Specs2
import org.specs2.{
  Specification,
  ScalaCheck
}

import org.specs2.matcher.DataTables
import org.specs2.scalaz.ValidationMatchers


class MixpanelAdapterSpec extends Specification with DataTables with ValidationMatchers with ScalaCheck {
  def is =
    "This is a specification to test the MixpanelAdapter functionality"                                        ^
                                                                                                              p^
      "toRawEvents must return a Success Nel if every 'users' in the payload is successful"                   ! e1^
      "toRawEvents must return a Nel Failure if the request body is missing"                                  ! e2^
      "toRawEvents must return a Nel Failure if the content type is missing"                                  ! e3^
      "toRawEvents must return a Nel Failure if the content type is incorrect"                                ! e4^
      "toRawEvents must return a Failure if the request body could not be parsed"                             ! e5^
      "toRawEvents must return a Failure if the request body does not contain an users parameter"             ! e6^
      "toRawEvents must return a Nel Failure if the event type is missing in querystring"                     ! e7^
                                                                                                              end

  implicit val resolver = SpecHelpers.IgluResolver

  object Shared {
    val api       = CollectorApi("com.mixpanel", "v1")
    val cljSource = CollectorSource("clj-tomcat", "UTF-8", None)
    val context   = CollectorContext(DateTime.parse("2016-09-07T00:18:48.000+00:00").some, "37.157.33.123".some, None, None, Nil, None)
  }

  val ContentType = Some("application/x-www-form-urlencoded")

  def e1 = {
    val body = """users=%5B%7B%22%24distinct_id%22%3A%20%22smith%40jane.com%22%2C%20%22%24properties%22%3A%20%7B%22%24name%22%3A%20%22Smith%20Jane%22%2C%20%22%24email%22%3A%20%22smith%40jane.com%22%2C%20%22Referring%20URL%22%3A%20%22http%3A%2F%2Fwww.google.com%22%7D%7D%2C%7B%22%24distinct_id%22%3A%20%22Asmith%40jane.com%22%2C%20%22%24properties%22%3A%20%7B%22%24name%22%3A%20%22ASmith%20Jane%22%2C%20%22%24email%22%3A%20%22Asmith%40jane.com%22%7D%7D%5D%0A"""
    val querystring = SpecHelpers.toNameValuePairs(("schema", "users"))
    val payload = CollectorPayload(Shared.api, querystring, ContentType, body.some, Shared.cljSource, Shared.context)
    val expected = NonEmptyList(
      RawEvent(
        Shared.api,
        Map("tv" -> "com.mixpanel-v1", "e"-> "ue", "p" -> "srv", "ue_pr" -> """{"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0","data":{"schema":"iglu:com.mixpanel/users/jsonschema/1-0-0","data":{"$distinct_id":"smith@jane.com","$properties":{"$name":"Smith Jane","$email":"smith@jane.com","ReferringURL":"http://www.google.com"}}}}"""),
        ContentType,
        Shared.cljSource,
        Shared.context
      ),
      RawEvent(
        Shared.api,
        Map("tv" -> "com.mixpanel-v1", "e"-> "ue", "p" -> "srv", "ue_pr" -> """{"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0","data":{"schema":"iglu:com.mixpanel/users/jsonschema/1-0-0","data":{"$distinct_id":"Asmith@jane.com","$properties":{"$name":"ASmith Jane","$email":"Asmith@jane.com"}}}}"""),
        ContentType,
        Shared.cljSource,
        Shared.context
      )
    )
    MixpanelAdapter.toRawEvents(payload) must beSuccessful(expected)
  }

  def e2 = {
    val querystring = SpecHelpers.toNameValuePairs(("schema", "users"))
    val payload = CollectorPayload(Shared.api, querystring, ContentType, None, Shared.cljSource, Shared.context)
    val expected = """Request body is empty: no Mixpanel events to process"""
    val actual = MixpanelAdapter.toRawEvents(payload)
    actual must beFailing(NonEmptyList(expected))
  }

  def e3 = {
    val contentType = None
    val body = """users=%5B%7B%22%24distinct_id%22%3A%20%22smith%40jane.com%22%2C%20%22%24properties%22%3A%20%7B%22%24name%22%3A%20%22Smith%20Jane%22%2C%20%22%24email%22%3A%20%22smith%40jane.com%22%7D%7D%2C%7B%22%24distinct_id%22%3A%20%22Asmith%40jane.com%22%2C%20%22%24properties%22%3A%20%7B%22%24name%22%3A%20%22ASmith%20Jane%22%2C%20%22%24email%22%3A%20%22Asmith%40jane.com%22%7D%7D%5D%0A"""
    val querystring = SpecHelpers.toNameValuePairs(("schema", "users"))
    val payload = CollectorPayload(Shared.api, querystring, contentType, body.some, Shared.cljSource, Shared.context)
    val expected = """Request body provided but content type empty, expected application/x-www-form-urlencoded for Mixpanel"""
    val actual = MixpanelAdapter.toRawEvents(payload)
    actual must beFailing(NonEmptyList(expected))
  }

  def e4 = {
    val contentType = Some("application/json")
    val body = """users=%5B%7B%22%24distinct_id%22%3A%20%22smith%40jane.com%22%2C%20%22%24properties%22%3A%20%7B%22%24name%22%3A%20%22Smith%20Jane%22%2C%20%22%24email%22%3A%20%22smith%40jane.com%22%7D%7D%2C%7B%22%24distinct_id%22%3A%20%22Asmith%40jane.com%22%2C%20%22%24properties%22%3A%20%7B%22%24name%22%3A%20%22ASmith%20Jane%22%2C%20%22%24email%22%3A%20%22Asmith%40jane.com%22%7D%7D%5D%0A"""
    val querystring = SpecHelpers.toNameValuePairs(("schema", "users"))
    val payload = CollectorPayload(Shared.api, querystring, contentType, body.some, Shared.cljSource, Shared.context)
    val expected = """Content type of application/json provided, expected application/x-www-form-urlencoded for Mixpanel"""
    val actual = MixpanelAdapter.toRawEvents(payload)
    actual must beFailing(NonEmptyList(expected))
  }

  def e5 = {
    val body = ""
    val querystring = SpecHelpers.toNameValuePairs(("schema", "userss"))
    val payload = CollectorPayload(Shared.api, querystring, ContentType, body.some, Shared.cljSource, Shared.context)
    val expectedJson = """Mixpanel request body does not have 'users' as a key: invalid event to process"""
    val actual = MixpanelAdapter.toRawEvents(payload)
    actual must beFailing(NonEmptyList(expectedJson))
  }

  def e6 = {
    val body = """users=%5B%7B%22%24distinct_id%22%3A%20%22smith%40jane.com%22%2C%20%22%24properties%22%3A%20%7B%22%24name%22%3A%20%22Smith%20Jane%22%2C%20%22%24email%22%3A%20%22smith%40jane.com%22%7D%7D%2C%7B%22%24distinct_id%22%3A%20%22Asmith%40jane.com%22%2C%20%22%24properties%22%20%7B%22%24name%22%3A%20%22ASmith%20Jane%22%2C%20%22%24email%22%3A%20%22Asmith%40jane.com%22%7D%7D%5D%0A%0A"""
    val querystring = SpecHelpers.toNameValuePairs(("schema", "users"))
    val payload = CollectorPayload(Shared.api, querystring, ContentType, body.some, Shared.cljSource, Shared.context)
    val expected = """Mixpanel event failed to parse into JSON: [com.fasterxml.jackson.core.JsonParseException: Unexpected character ('{' (code 123)): was expecting a colon to separate field name and value at [Source: java.io.StringReader@xxxxxx; line: 1, column: 156]]"""
    val actual = MixpanelAdapter.toRawEvents(payload)
    actual must beFailing(NonEmptyList(expected))
  }

  def e7 = {
    val body = """users=%5B%7B%22%24distinct_id%22%3A%20%22smith%40jane.com%22%2C%20%22%24properties%22%3A%20%7B%22%24name%22%3A%20%22Smith%20Jane%22%2C%20%22%24email%22%3A%20%22smith%40jane.com%22%7D%7D%2C%7B%22%24distinct_id%22%3A%20%22Asmith%40jane.com%22%2C%20%22%24properties%22%20%7B%22%24name%22%3A%20%22ASmith%20Jane%22%2C%20%22%24email%22%3A%20%22Asmith%40jane.com%22%7D%7D%5D%0A%0A"""
    val payload = CollectorPayload(Shared.api, Nil, ContentType, body.some, Shared.cljSource, Shared.context)
    val expected = """No Mixpanel schema type provided in querystring: cannot determine event type"""
    val actual = MixpanelAdapter.toRawEvents(payload)
    actual must beFailing(NonEmptyList(expected))
  }
}
