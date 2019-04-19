/*
 * Copyright (c) 2018 Snowplow Analytics Ltd. All rights reserved.
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

class MarketoAdapterSpec extends Specification with DataTables with ValidationMatchers with ScalaCheck {
  def is = s2"""
  This is a specification to test the MarketoAdapter functionality
  toRawEvents must return a success for a valid "event" type payload body being passed                $e1
  toRawEvents must return a Failure Nel if the payload body is empty                                  $e2
  """

  implicit val resolver = SpecHelpers.IgluResolver

  object Shared {
    val api       = CollectorApi("com.marketo", "v1")
    val cljSource = CollectorSource("clj-tomcat", "UTF-8", None)
    val context = CollectorContext(DateTime.parse("2018-01-01T00:00:00.000+00:00").some,
                                   "37.157.33.123".some,
                                   None,
                                   None,
                                   Nil,
                                   None)
  }

  val ContentType = "application/json"

  def e1 = {
    val bodyStr =
      """{"name": "webhook for A", "step": 6, "campaign": {"id": 160, "name": "avengers assemble"}, "lead": {"acquisition_date": "2010-11-11 11:11:11", "black_listed": false, "first_name": "the hulk", "updated_at": "", "created_at": "2018-06-16 11:23:58", "last_interesting_moment_date": "2018-09-26 20:26:40"}, "company": {"name": "iron man", "notes": "the something dog leapt over the lazy fox"}, "campaign": {"id": 987, "name": "triggered event"}, "datetime": "2018-03-07 14:28:16"}"""
    val payload = CollectorPayload(Shared.api, Nil, ContentType.some, bodyStr.some, Shared.cljSource, Shared.context)
    val expected = NonEmptyList(
      RawEvent(
        Shared.api,
        Map(
          "tv"    -> "com.marketo-v1",
          "e"     -> "ue",
          "p"     -> "srv",
          "ue_pr" -> """{"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0","data":{"schema":"iglu:com.marketo/event/jsonschema/2-0-0","data":{"name":"webhook for A","step":6,"campaign":{"id":160,"name":"avengers assemble"},"lead":{"acquisition_date":"2010-11-11T11:11:11.000Z","black_listed":false,"first_name":"the hulk","updated_at":"","created_at":"2018-06-16T11:23:58.000Z","last_interesting_moment_date":"2018-09-26T20:26:40.000Z"},"company":{"name":"iron man","notes":"the something dog leapt over the lazy fox"},"campaign":{"id":987,"name":"triggered event"},"datetime":"2018-03-07T14:28:16.000Z"}}}"""
        ),
        ContentType.some,
        Shared.cljSource,
        Shared.context
      ))
    MarketoAdapter.toRawEvents(payload) must beSuccessful(expected)
  }

  def e2 = {
    val payload = CollectorPayload(Shared.api, Nil, ContentType.some, None, Shared.cljSource, Shared.context)
    MarketoAdapter.toRawEvents(payload) must beFailing(
      NonEmptyList("Request body is empty: no Marketo event to process"))
  }
}
