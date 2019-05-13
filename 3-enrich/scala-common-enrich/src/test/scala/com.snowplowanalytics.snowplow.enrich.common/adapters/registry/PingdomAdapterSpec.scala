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
import com.snowplowanalytics.snowplow.badrows._
import io.circe.literal._
import org.joda.time.DateTime
import org.specs2.Specification
import org.specs2.matcher.{DataTables, ValidatedMatchers}

import loaders._
import utils.Clock._

class PingdomAdapterSpec extends Specification with DataTables with ValidatedMatchers {
  def is = s2"""
  This is a specification to test the PingdomAdapter functionality
  reformatParameters should return either an updated JSON without the 'action' field or the same JSON $e1
  reformatMapParams must return a Failure Nel for any Python Unicode wrapped values                   $e2
  toRawEvents must return a Success Nel for a valid querystring                                       $e3
  toRawEvents must return a Failure Nel for an empty querystring                                      $e4
  toRawEvents must return a Failure Nel for a querystring which does not contain 'message' as a key   $e5
  """

  object Shared {
    val api = CollectorApi("com.pingdom", "v1")
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

  def e1 =
    "SPEC NAME" || "JSON" | "EXPECTED OUTPUT" |
      "Remove action field" !! json"""{"action":"assign","agent":"smith"}""" ! json"""{"agent":"smith"}""" |
      "Nothing removed" !! json"""{"actions":"assign","agent":"smith"}""" ! json"""{"actions":"assign","agent":"smith"}""" |> {
      (_, json, expected) =>
        PingdomAdapter.reformatParameters(json) mustEqual expected
    }

  def e2 = {
    val nvPairs = SpecHelpers.toNameValuePairs("p" -> "(u'apps',)")
    val expected =
      FailureDetails.AdapterFailure.InputData(
        "p",
        "apps".some,
        """should not pass regex \(u'(.+)',\)"""
      )
    PingdomAdapter.reformatMapParams(nvPairs) must beLeft(NonEmptyList.one(expected))
  }

  def e3 = {
    val querystring = SpecHelpers.toNameValuePairs(
      "p" -> "apps",
      "message" -> """{"check": "1421338", "checkname": "Webhooks_Test", "host": "7eef51c2.ngrok.com", "action": "assign", "incidentid": 3, "description": "down"}"""
    )
    val payload =
      CollectorPayload(Shared.api, querystring, None, None, Shared.cljSource, Shared.context)
    val expected = RawEvent(
      Shared.api,
      Map(
        "tv" -> "com.pingdom-v1",
        "e" -> "ue",
        "p" -> "apps",
        "ue_pr" -> """{"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0","data":{"schema":"iglu:com.pingdom/incident_assign/jsonschema/1-0-0","data":{"check":"1421338","checkname":"Webhooks_Test","host":"7eef51c2.ngrok.com","incidentid":3,"description":"down"}}}"""
      ),
      None,
      Shared.cljSource,
      Shared.context
    )
    PingdomAdapter.toRawEvents(payload, SpecHelpers.client).value must beValid(
      NonEmptyList.one(expected)
    )
  }

  def e4 = {
    val payload = CollectorPayload(Shared.api, Nil, None, None, Shared.cljSource, Shared.context)
    val expected =
      FailureDetails.AdapterFailure.InputData(
        "querystring",
        None,
        "empty querystring: no events to process"
      )
    PingdomAdapter.toRawEvents(payload, SpecHelpers.client).value must beInvalid(
      NonEmptyList.one(expected)
    )
  }

  def e5 = {
    val querystring = SpecHelpers.toNameValuePairs("p" -> "apps")
    val payload =
      CollectorPayload(Shared.api, querystring, None, None, Shared.cljSource, Shared.context)
    val expected =
      FailureDetails.AdapterFailure.InputData(
        "querystring",
        "p=apps".some,
        "no `message` parameter provided"
      )
    PingdomAdapter.toRawEvents(payload, SpecHelpers.client).value must beInvalid(
      NonEmptyList.one(expected)
    )
  }
}
