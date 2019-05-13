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

class PagerdutyAdapterSpec extends Specification with DataTables with ValidatedMatchers {
  def is = s2"""
  This is a specification to test the PagerdutyAdapter functionality
  reformatParameters must return an updated JSON whereby all null Strings have been replaced by null $e1
  reformatParameters must return an updated JSON where 'incident.xxx' is replaced by xxx             $e2
  reformatParameters must return an updated JSON whereby all invalid datetime strings are corrected  $e3
  payloadBodyToEvents must return a Success list of event JSON's from a valid payload body           $e4
  payloadBodyToEvents must return a Failure Nel for an invalid payload body being passed             $e5
  toRawEvents must return a Success Nel if all events are successful                                 $e6
  toRawEvents must return a Failure Nel if any of the events were not successes                     $e7
  toRawEvents must return a Nel Failure if the request body is missing                               $e8
  toRawEvents must return a Nel Failure if the content type is missing                               $e9
  toRawEvents must return a Nel Failure if the content type is incorrect                             $e10
  """

  object Shared {
    val api = CollectorPayload.Api("com.pagerduty", "v1")
    val cljSource = CollectorPayload.Source("clj-tomcat", "UTF-8", None)
    val context = CollectorPayload.Context(
      DateTime.parse("2013-08-29T00:18:48.000+00:00").some,
      "37.157.33.123".some,
      None,
      None,
      Nil,
      None
    )
  }

  val ContentType = "application/json"

  def e1 =
    "SPEC NAME" || "INPUT" | "EXPECTED OUTPUT" |
      "Valid, update one value" !! json"""{"type":"null"}""" ! json"""{"type":null}""" |
      "Valid, update multiple values" !! json"""{"type":"null","some":"null"}""" ! json"""{"type":null,"some":null}""" |
      "Valid, update nested values" !! json"""{"type": {"some":"null"}}""" ! json"""{"type":{"some":null}}""" |> {
      (_, input, expected) =>
        PagerdutyAdapter.reformatParameters(input) mustEqual expected
    }

  def e2 = {
    val json = json"""{"type":"incident.trigger"}"""
    val expected = json"""{"type":"trigger"}"""
    PagerdutyAdapter.reformatParameters(json) mustEqual expected
  }

  def e3 =
    "SPEC NAME" || "INPUT" | "EXPECTED OUTPUT" |
      "Valid, update one value" !! json"""{"created_on":"2014-11-12T18:53:47 00:00"}""" ! json"""{"created_on":"2014-11-12T18:53:47+00:00"}""" |
      "Valid, update multiple values" !! json"""{"created_on":"2014-11-12T18:53:47 00:00","last_status_change_on":"2014-11-12T18:53:47 00:00"}""" ! json"""{"created_on":"2014-11-12T18:53:47+00:00","last_status_change_on":"2014-11-12T18:53:47+00:00"}""" |
      "Valid, update nested values" !! json"""{"created_on":"2014-12-15T08:19:54Z","nested":{"created_on":"2014-11-12T18:53:47 00:00"}}""" ! json"""{"created_on":"2014-12-15T08:19:54Z","nested":{"created_on":"2014-11-12T18:53:47+00:00"}}""" |> {
      (_, input, expected) =>
        PagerdutyAdapter.reformatParameters(input) mustEqual expected
    }

  def e4 = {
    val bodyStr =
      """{"messages":[{"type":"incident.trigger","data":{"incident":{"id":"P9WY9U9"}}}]}"""
    val expected = List(json"""{
      "type": "incident.trigger",
      "data": {
        "incident": {
          "id": "P9WY9U9"
        }
      }
    }""")
    PagerdutyAdapter.payloadBodyToEvents(bodyStr) must beRight(expected)
  }

  def e5 =
    "SPEC NAME" || "INPUT" | "EXPECTED OUTPUT" |
      "Failure, parse exception" !! """{"something:"some"}""" ! FailureDetails.AdapterFailure
        .NotJson(
          "body",
          """{"something:"some"}""".some,
          """invalid json: expected : got 'some"}' (line 1, column 14)"""
        ) |
      "Failure, missing messages key" !! """{"somekey":"key"}""" ! FailureDetails.AdapterFailure
        .InputData(
          "messages",
          """{"somekey":"key"}""".some,
          "field `messages` is not an array"
        ) |> { (_, input, expected) =>
      PagerdutyAdapter.payloadBodyToEvents(input) must beLeft(expected)
    }

  def e6 = {
    val bodyStr =
      """{"messages":[{"type":"incident.trigger","data":{"incident":{"id":"P9WY9U9","incident_number":139,"created_on":"2014-11-12T18:53:47 00:00","status":"triggered","html_url":"https://snowplow.pagerduty.com/incidents/P9WY9U9","incident_key":"srv01/HTTP","service":{"id":"PTHO4FF","name":"Webhooks Test","html_url":"https://snowplow.pagerduty.com/services/PTHO4FF","deleted_at":null},"escalation_policy":{"id":"P8ETVHU","name":"Default","deleted_at":null},"assigned_to_user":{"id":"P9L426X","name":"Yali Sassoon","email":"yali@snowplowanalytics.com","html_url":"https://snowplow.pagerduty.com/users/P9L426X"},"trigger_summary_data":{"description":"FAILURE for production/HTTP on machine srv01.acme.com","client":"Sample Monitoring Service","client_url":"https://monitoring.service.com"},"trigger_details_html_url":"https://snowplow.pagerduty.com/incidents/P9WY9U9/log_entries/P5AWPTR","trigger_type":"trigger_svc_event","last_status_change_on":"2014-11-12T18:53:47Z","last_status_change_by":null,"number_of_escalations":0,"assigned_to":[{"at":"2014-11-12T18:53:47Z","object":{"id":"P9L426X","name":"Yali Sassoon","email":"yali@snowplowanalytics.com","html_url":"https://snowplow.pagerduty.com/users/P9L426X","type":"user"}}]}},"id":"3c3e8ee0-6a9d-11e4-b3d5-22000ae31361","created_on":"2014-11-12T18:53:47Z"}]}"""
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
          "tv" -> "com.pagerduty-v1",
          "e" -> "ue",
          "p" -> "srv",
          "ue_pr" -> """{"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0","data":{"schema":"iglu:com.pagerduty/incident/jsonschema/1-0-0","data":{"type":"trigger","data":{"incident":{"assigned_to_user":{"id":"P9L426X","name":"Yali Sassoon","email":"yali@snowplowanalytics.com","html_url":"https://snowplow.pagerduty.com/users/P9L426X"},"incident_key":"srv01/HTTP","trigger_summary_data":{"description":"FAILURE for production/HTTP on machine srv01.acme.com","client":"Sample Monitoring Service","client_url":"https://monitoring.service.com"},"last_status_change_by":null,"incident_number":139,"service":{"id":"PTHO4FF","name":"Webhooks Test","html_url":"https://snowplow.pagerduty.com/services/PTHO4FF","deleted_at":null},"trigger_details_html_url":"https://snowplow.pagerduty.com/incidents/P9WY9U9/log_entries/P5AWPTR","id":"P9WY9U9","assigned_to":[{"at":"2014-11-12T18:53:47Z","object":{"id":"P9L426X","name":"Yali Sassoon","email":"yali@snowplowanalytics.com","html_url":"https://snowplow.pagerduty.com/users/P9L426X","type":"user"}}],"number_of_escalations":0,"last_status_change_on":"2014-11-12T18:53:47Z","status":"triggered","escalation_policy":{"id":"P8ETVHU","name":"Default","deleted_at":null},"created_on":"2014-11-12T18:53:47+00:00","trigger_type":"trigger_svc_event","html_url":"https://snowplow.pagerduty.com/incidents/P9WY9U9"}},"id":"3c3e8ee0-6a9d-11e4-b3d5-22000ae31361","created_on":"2014-11-12T18:53:47Z"}}}"""
        ),
        ContentType.some,
        Shared.cljSource,
        Shared.context
      )
    )
    PagerdutyAdapter.toRawEvents(payload, SpecHelpers.client).value must beValid(expected)
  }

  def e7 = {
    val bodyStr =
      """{"messages":[{"type":"trigger","data":{"incident":{"id":"P9WY9U9","incident_number":139,"created_on":"2014-11-12T18:53:47 00:00","status":"triggered","html_url":"https://snowplow.pagerduty.com/incidents/P9WY9U9","incident_key":"srv01/HTTP","service":{"id":"PTHO4FF","name":"Webhooks Test","html_url":"https://snowplow.pagerduty.com/services/PTHO4FF","deleted_at":null},"escalation_policy":{"id":"P8ETVHU","name":"Default","deleted_at":null},"assigned_to_user":{"id":"P9L426X","name":"Yali Sassoon","email":"yali@snowplowanalytics.com","html_url":"https://snowplow.pagerduty.com/users/P9L426X"},"trigger_summary_data":{"description":"FAILURE for production/HTTP on machine srv01.acme.com","client":"Sample Monitoring Service","client_url":"https://monitoring.service.com"},"trigger_details_html_url":"https://snowplow.pagerduty.com/incidents/P9WY9U9/log_entries/P5AWPTR","trigger_type":"trigger_svc_event","last_status_change_on":"2014-11-12T18:53:47Z","last_status_change_by":null,"number_of_escalations":0,"assigned_to":[{"at":"2014-11-12T18:53:47Z","object":{"id":"P9L426X","name":"Yali Sassoon","email":"yali@snowplowanalytics.com","html_url":"https://snowplow.pagerduty.com/users/P9L426X","type":"user"}}]}},"id":"3c3e8ee0-6a9d-11e4-b3d5-22000ae31361","created_on":"2014-11-12T18:53:47Z"}]}"""
    val payload = CollectorPayload(
      Shared.api,
      Nil,
      ContentType.some,
      bodyStr.some,
      Shared.cljSource,
      Shared.context
    )
    val expected = FailureDetails.AdapterFailure.SchemaMapping(
      "trigger".some,
      PagerdutyAdapter.EventSchemaMap,
      "no schema associated with the provided type parameter at index 0"
    )
    PagerdutyAdapter.toRawEvents(payload, SpecHelpers.client).value must beInvalid(
      NonEmptyList.one(expected)
    )
  }

  def e8 = {
    val payload =
      CollectorPayload(Shared.api, Nil, ContentType.some, None, Shared.cljSource, Shared.context)
    PagerdutyAdapter.toRawEvents(payload, SpecHelpers.client).value must beInvalid(
      NonEmptyList.one(
        FailureDetails.AdapterFailure
          .InputData("body", None, "empty body: no events to process")
      )
    )
  }

  def e9 = {
    val payload =
      CollectorPayload(Shared.api, Nil, None, "stub".some, Shared.cljSource, Shared.context)
    PagerdutyAdapter.toRawEvents(payload, SpecHelpers.client).value must beInvalid(
      NonEmptyList.one(
        FailureDetails.AdapterFailure.InputData(
          "contentType",
          None,
          "no content type: expected application/json"
        )
      )
    )
  }

  def e10 = {
    val ct = "application/x-www-form-urlencoded".some
    val payload = CollectorPayload(
      Shared.api,
      Nil,
      ct,
      "stub".some,
      Shared.cljSource,
      Shared.context
    )
    PagerdutyAdapter.toRawEvents(payload, SpecHelpers.client).value must beInvalid(
      NonEmptyList.one(
        FailureDetails.AdapterFailure
          .InputData("contentType", ct, "expected application/json")
      )
    )
  }
}
