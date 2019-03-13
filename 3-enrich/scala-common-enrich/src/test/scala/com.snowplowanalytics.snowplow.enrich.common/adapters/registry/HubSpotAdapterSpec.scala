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

import io.circe.literal._
import org.joda.time.DateTime
import org.specs2.{ScalaCheck, Specification}
import org.specs2.matcher.DataTables
import org.specs2.scalaz.ValidationMatchers
import scalaz._
import Scalaz._

import loaders.{CollectorApi, CollectorContext, CollectorPayload, CollectorSource}

class HubSpotAdapterSpec
    extends Specification
    with DataTables
    with ValidationMatchers
    with ScalaCheck {
  def is = s2"""
  This is a specification to test the HubSpotAdapter functionality
  payloadBodyToEvents must return a Success list of event JSON's from a valid payload body           $e1
  payloadBodyToEvents must return a Failure Nel for an invalid payload body being passed             $e2
  toRawEvents must return a Success Nel if all events are successful                                 $e3
  toRawEvents must return a Failure Nel if any of the events where not successes                     $e4
  toRawEvents must return a Nel Failure if the request body is missing                               $e5
  toRawEvents must return a Nel Failure if the content type is missing                               $e6
  toRawEvents must return a Nel Failure if the content type is incorrect                             $e7
  """

  implicit val resolver = SpecHelpers.IgluResolver

  object Shared {
    val api = CollectorApi("com.hubspot", "v1")
    val cljSource = CollectorSource("clj-tomcat", "UTF-8", None)
    val context = CollectorContext(
      DateTime.parse("2013-08-29T00:18:48.000+00:00").some,
      "37.157.33.123".some,
      None,
      None,
      Nil,
      None)
  }

  val ContentType = "application/json"

  def e1 = {
    val bodyStr = """[{"subscriptionType":"company.change","eventId":16}]"""
    val expected = json"""{
      "subscriptionType": "company.change",
      "eventId": 16
    }"""
    HubSpotAdapter.payloadBodyToEvents(bodyStr) must beSuccessful(List(expected))
  }

  def e2 =
    "SPEC NAME" || "INPUT" | "EXPECTED OUTPUT" |
      "Failure, parse exception" !! """{"something:"some"}""" ! """HubSpot payload failed to parse into JSON: [expected : got 'some"}' (line 1, column 14)]""" |> {
      (_, input, expected) =>
        HubSpotAdapter.payloadBodyToEvents(input) must beFailing(expected)
    }

  def e3 = {
    val bodyStr =
      """[{"eventId":1,"subscriptionId":25458,"portalId":4737818,"occurredAt":1539145399845,"subscriptionType":"contact.creation","attemptNumber":0,"objectId":123,"changeSource":"CRM","changeFlag":"NEW","appId":177698}]"""
    val payload = CollectorPayload(
      Shared.api,
      Nil,
      ContentType.some,
      bodyStr.some,
      Shared.cljSource,
      Shared.context)
    val expected = NonEmptyList(
      RawEvent(
        Shared.api,
        Map(
          "tv" -> "com.hubspot-v1",
          "e" -> "ue",
          "p" -> "srv",
          "ue_pr" -> """{"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0","data":{"schema":"iglu:com.hubspot/contact_creation/jsonschema/1-0-0","data":{"eventId":1,"subscriptionId":25458,"portalId":4737818,"occurredAt":"2018-10-10T04:23:19.845Z","attemptNumber":0,"objectId":123,"changeSource":"CRM","changeFlag":"NEW","appId":177698}}}"""
        ),
        ContentType.some,
        Shared.cljSource,
        Shared.context
      ))
    HubSpotAdapter.toRawEvents(payload) must beSuccessful(expected)
  }

  def e4 = {
    val bodyStr =
      """[{"eventId":1,"subscriptionId":25458,"portalId":4737818,"occurredAt":1539145399845,"subscriptionType":"contact","attemptNumber":0,"objectId":123,"changeSource":"CRM","changeFlag":"NEW","appId":177698}]"""
    val payload = CollectorPayload(
      Shared.api,
      Nil,
      ContentType.some,
      bodyStr.some,
      Shared.cljSource,
      Shared.context)
    val expected = "HubSpot event at index [0] failed: type parameter [contact] not recognized"
    HubSpotAdapter.toRawEvents(payload) must beFailing(NonEmptyList(expected))
  }

  def e5 = {
    val payload =
      CollectorPayload(Shared.api, Nil, ContentType.some, None, Shared.cljSource, Shared.context)
    HubSpotAdapter.toRawEvents(payload) must beFailing(
      NonEmptyList("Request body is empty: no HubSpot events to process"))
  }

  def e6 = {
    val payload =
      CollectorPayload(Shared.api, Nil, None, "stub".some, Shared.cljSource, Shared.context)
    HubSpotAdapter.toRawEvents(payload) must beFailing(
      NonEmptyList(
        "Request body provided but content type empty, expected application/json for HubSpot"))
  }

  def e7 = {
    val payload = CollectorPayload(
      Shared.api,
      Nil,
      "application/x-www-form-urlencoded".some,
      "stub".some,
      Shared.cljSource,
      Shared.context)
    HubSpotAdapter.toRawEvents(payload) must beFailing(NonEmptyList(
      "Content type of application/x-www-form-urlencoded provided, expected application/json for HubSpot"))
  }
}
