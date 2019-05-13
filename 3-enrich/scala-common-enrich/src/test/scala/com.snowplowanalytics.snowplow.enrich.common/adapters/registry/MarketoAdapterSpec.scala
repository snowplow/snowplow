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

import cats.data.NonEmptyList
import cats.syntax.option._
import com.snowplowanalytics.snowplow.badrows._
import org.joda.time.DateTime
import org.specs2.Specification
import org.specs2.matcher.{DataTables, ValidatedMatchers}

import loaders._
import utils.Clock._

class MarketoAdapterSpec extends Specification with DataTables with ValidatedMatchers {
  def is = s2"""
  This is a specification to test the MarketoAdapter functionality
  toRawEvents must return a success for a valid "event" type payload body being passed                $e1
  toRawEvents must return a Failure Nel if the payload body is empty                                  $e2
  """

  object Shared {
    val api = CollectorPayload.Api("com.marketo", "v1")
    val cljSource = CollectorPayload.Source("clj-tomcat", "UTF-8", None)
    val context = CollectorPayload.Context(
      DateTime.parse("2018-01-01T00:00:00.000+00:00").some,
      "37.157.33.123".some,
      None,
      None,
      Nil,
      None
    )
  }

  val ContentType = "application/json"

  def e1 = {
    val bodyStr =
      """{"name": "webhook for A", "step": 6, "lead": {"acquisition_date": "2010-11-11 11:11:11", "black_listed": false, "first_name": "the hulk", "updated_at": "", "created_at": "2018-06-16 11:23:58", "last_interesting_moment_date": "2018-09-26 20:26:40"}, "company": {"name": "iron man", "notes": "the something dog leapt over the lazy fox"}, "campaign": {"id": 987, "name": "triggered event"}, "datetime": "2018-03-07 14:28:16"}"""
    val payload = CollectorPayload(
      Shared.api,
      Nil,
      ContentType.some,
      bodyStr.some,
      Shared.cljSource,
      Shared.context
    )
    val expected = NonEmptyList.one(
      RawEvent(
        Shared.api,
        Map(
          "tv" -> "com.marketo-v1",
          "e" -> "ue",
          "p" -> "srv",
          "ue_pr" -> """{"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0","data":{"schema":"iglu:com.marketo/event/jsonschema/2-0-0","data":{"lead":{"first_name":"the hulk","acquisition_date":"2010-11-11T11:11:11.000Z","black_listed":false,"last_interesting_moment_date":"2018-09-26T20:26:40.000Z","created_at":"2018-06-16T11:23:58.000Z","updated_at":""},"name":"webhook for A","step":6,"campaign":{"id":987,"name":"triggered event"},"datetime":"2018-03-07T14:28:16.000Z","company":{"name":"iron man","notes":"the something dog leapt over the lazy fox"}}}}"""
        ),
        ContentType.some,
        Shared.cljSource,
        Shared.context
      )
    )
    MarketoAdapter.toRawEvents(payload, SpecHelpers.client).value must beValid(expected)
  }

  def e2 = {
    val payload =
      CollectorPayload(Shared.api, Nil, ContentType.some, None, Shared.cljSource, Shared.context)
    MarketoAdapter.toRawEvents(payload, SpecHelpers.client).value must beInvalid(
      NonEmptyList.one(
        FailureDetails.AdapterFailure
          .InputData("body", None, "empty body: no events to process")
      )
    )
  }
}
