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
import SpecHelpers._

// Specs2
import org.specs2.{Specification, ScalaCheck}
import org.specs2.matcher.DataTables
import org.specs2.scalaz.ValidationMatchers

class SnowplowAdapterSpec extends Specification with DataTables with ValidationMatchers with ScalaCheck { def is =

  "This is a specification to test the SnowplowAdapter functionality"                                                   ^
                                                                                                                       p^
  "Tp1.toRawEvents should return a NEL containing one RawEvent if the querystring is populated"                         ! e1^
  "Tp1.toRawEvents should return a Validation Failure if the querystring is empty"                                      ! e2^
  "Tp2.toRawEvents should return a Validation Failure if the content type is incorrect"                                 ! e3^
  "Tp2.toRawEvents should return a Validation Failure if neither querystring nor body are populated"                    ! e4^
  "Tp2.toRawEvents should return a Validation Failure if body is populated but content type is missing"                 ! e5^
  "Tp2.toRawEvents should return a Validation Failure if (correct) content type is provided but body is missing"        ! e6^
  "Tp2.toRawEvents should return a Validation Failure if the body is not a JSON"                                        ! e7^
                                                                                                                        end

  // TODO: add a test where payload validates but is wrong schema
  // TODO: add a test where payload fails schema validation
  // TODO: add a test where a payload -> N events
  // TODO: add a test where a qs field is added to payload
  // TODO: add a test where a qs field overwrites a body field

  implicit val resolver = SpecHelpers.IgluResolver

  object Expected {
    val api: (String) => CollectorApi = version => CollectorApi("com.snowplowanalytics.snowplow", version)
    val source = CollectorSource("clj-tomcat", "UTF-8", None)
    val context = CollectorContext(DateTime.parse("2013-08-29T00:18:48.000+00:00"), "37.157.33.123".some, None, None, Nil, None)
  }

  def e1 = {
    val api = Expected.api("tp1")
    val payload = CollectorPayload(api, toNameValuePairs("aid" -> "test"), None, None, Expected.source, Expected.context)
    val actual = SnowplowAdapter.Tp1.toRawEvents(payload)
    actual must beSuccessful(NonEmptyList(RawEvent(api, Map("aid" -> "test"), None, Expected.source, Expected.context)))
  }

  def e2 = {
  	val payload = CollectorPayload(Expected.api("tp1"), Nil, None, None, Expected.source, Expected.context)
  	val actual = SnowplowAdapter.Tp1.toRawEvents(payload)
  	actual must beFailing(NonEmptyList("Querystring is empty: no raw event to process"))
  }

  def e3 = {
    val payload = CollectorPayload(Expected.api("tp2"), Nil, "text/plain".some, "body".some, Expected.source, Expected.context)
    val actual = SnowplowAdapter.Tp2.toRawEvents(payload)
    actual must beFailing(NonEmptyList("Content type of text/plain provided, expected application/json; charset=utf-8"))
  }

  def e4 = {
    val payload = CollectorPayload(Expected.api("tp2"), Nil, None, None, Expected.source, Expected.context)
    val actual = SnowplowAdapter.Tp2.toRawEvents(payload)
    actual must beFailing(NonEmptyList("Request body and querystring parameters empty, expected at least one populated"))
  }

  def e5 = {
    val payload = CollectorPayload(Expected.api("tp2"), Nil, None, "body".some, Expected.source, Expected.context)
    val actual = SnowplowAdapter.Tp2.toRawEvents(payload)
    actual must beFailing(NonEmptyList("Request body provided but content type empty, expected application/json; charset=utf-8"))
  }

  def e6 = {
    val payload = CollectorPayload(Expected.api("tp2"), toNameValuePairs("aid" -> "test"), "application/json; charset=utf-8".some, None, Expected.source, Expected.context)
    val actual = SnowplowAdapter.Tp2.toRawEvents(payload)
    actual must beFailing(NonEmptyList("Content type of application/json; charset=utf-8 provided but request body empty"))
  }

  def e7 = {
    val payload = CollectorPayload(Expected.api("tp2"), toNameValuePairs("aid" -> "test"), "application/json; charset=utf-8".some, "<<not a json>>".some, Expected.source, Expected.context)
    val actual = SnowplowAdapter.Tp2.toRawEvents(payload)
    actual must beFailing(NonEmptyList("Field [Body]: invalid JSON [<<not a json>>] with parsing error: Unexpected character ('<' (code 60)): expected a valid value (number, String, array, object, 'true', 'false' or 'null') at [Source: java.io.StringReader@xxxxxx; line: 1, column: 2]"))
  }
}