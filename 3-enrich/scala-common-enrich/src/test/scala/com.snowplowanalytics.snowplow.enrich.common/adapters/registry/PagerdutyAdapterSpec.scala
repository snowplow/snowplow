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

// json4s
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import org.json4s.scalaz.JsonScalaz._

// Snowplow
import loaders.{
  CollectorApi,
  CollectorSource,
  CollectorContext,
  CollectorPayload
}
import utils.ConversionUtils
import SpecHelpers._

// Specs2
import org.specs2.{Specification, ScalaCheck}
import org.specs2.matcher.DataTables
import org.specs2.scalaz.ValidationMatchers

class PagerdutyAdapterSpec extends Specification with DataTables with ValidationMatchers with ScalaCheck { def is =

  "This is a specification to test the PagerdutyAdapter functionality"                                              ^
                                                                                                                   p^
  "reformatParameters must return an updated JSON whereby all null Strings have been replaced by null"            ! e1^
  "reformatParameters must return an updated JSON where 'incident.xxx' is replaced by xxx"                        ! e2^
  "reformatParameters must return an updated JSON whereby all invalid datetime strings are corrected"             ! e3^
  "payloadBodyToEvents must return a Success list of event JSON's from a valid payload body"                      ! e4^
  "payloadBodyToEvents must return a Failure Nel for an invalid payload body being passed"                        ! e5^
  "toRawEvents must return a Success Nel if all events are successful"                                            ! e6^
  "toRawEvents must return a Failure Nel if any of the events where not successes"                                ! e7^
  "toRawEvents must return a Nel Failure if the request body is missing"                                          ! e8^
  "toRawEvents must return a Nel Failure if the content type is missing"                                          ! e9^
  "toRawEvents must return a Nel Failure if the content type is incorrect"                                        ! e10^
                                                                                                                   end
  implicit val resolver = SpecHelpers.IgluResolver

  object Shared {
    val api = CollectorApi("com.pagerduty", "v1")
    val cljSource = CollectorSource("clj-tomcat", "UTF-8", None)
    val context = CollectorContext(DateTime.parse("2013-08-29T00:18:48.000+00:00").some, "37.157.33.123".some, None, None, Nil, None)
  }

  val ContentType = "application/json"

  def e1 = 
    "SPEC NAME"                     || "INPUT"                             | "EXPECTED OUTPUT"               |
    "Valid, update one value"       !! """{"type":"null"}"""               ! """{"type":null}"""             |
    "Valid, update multiple values" !! """{"type":"null","some":"null"}""" ! """{"type":null,"some":null}""" |
    "Valid, update nested values"   !! """{"type": {"some":"null"}}"""     ! """{"type":{"some":null}}"""    |> {
      (_, input, expected) => PagerdutyAdapter.reformatParameters(parse(input)) mustEqual parse(expected)
  }

  def e2 = {
    val json = parse("""{"type":"incident.trigger"}""")
    val expected = parse("""{"type":"trigger"}""")
    PagerdutyAdapter.reformatParameters(json) mustEqual expected
  }

  def e3 = 
    "SPEC NAME"                     || "INPUT"                                                                                              | "EXPECTED OUTPUT"                                                                                    |
    "Valid, update one value"       !! """{"created_on":"2014-11-12T18:53:47 00:00"}"""                                                     ! """{"created_on":"2014-11-12T18:53:47+00:00"}"""                                                     |
    "Valid, update multiple values" !! """{"created_on":"2014-11-12T18:53:47 00:00","last_status_change_on":"2014-11-12T18:53:47 00:00"}""" ! """{"created_on":"2014-11-12T18:53:47+00:00","last_status_change_on":"2014-11-12T18:53:47+00:00"}""" |
    "Valid, update nested values"   !! """{"created_on":"2014-12-15T08:19:54Z","nested":{"created_on":"2014-11-12T18:53:47 00:00"}}"""      ! """{"created_on":"2014-12-15T08:19:54Z","nested":{"created_on":"2014-11-12T18:53:47+00:00"}}"""      |> {
      (_, input, expected) => PagerdutyAdapter.reformatParameters(parse(input)) mustEqual parse(expected)
  }

  def e4 = {
    val bodyStr = """{"messages":[{"type":"incident.trigger","data":{"incident":{"id":"P9WY9U9"}}}]}"""
    val expected = List(JObject(List(("type",JString("incident.trigger")), ("data",JObject(List(("incident",JObject(List(("id",JString("P9WY9U9")))))))))))
    PagerdutyAdapter.payloadBodyToEvents(bodyStr) must beSuccessful(expected)
  }

  def e5 = 
    "SPEC NAME"                     || "INPUT"                   | "EXPECTED OUTPUT"                                              |
    "Failure, parse exception"      !! """{"something:"some"}""" ! "PagerDuty payload failed to parse into JSON: [com.fasterxml.jackson.core.JsonParseException: Unexpected character ('s' (code 115)): was expecting a colon to separate field name and value at [Source: java.io.StringReader@xxxxxx; line: 1, column: 15]]"     |
    "Failure, missing messages key" !! """{"somekey":"key"}"""   ! "PagerDuty payload does not contain the needed 'messages' key" |> {
      (_, input, expected) => PagerdutyAdapter.payloadBodyToEvents(input) must beFailing(expected) 
    }

  def e6 = {
    val bodyStr = """{"messages":[{"type":"incident.trigger","data":{"incident":{"id":"P9WY9U9","incident_number":139,"created_on":"2014-11-12T18:53:47 00:00","status":"triggered","html_url":"https://snowplow.pagerduty.com/incidents/P9WY9U9","incident_key":"srv01/HTTP","service":{"id":"PTHO4FF","name":"Webhooks Test","html_url":"https://snowplow.pagerduty.com/services/PTHO4FF","deleted_at":null},"escalation_policy":{"id":"P8ETVHU","name":"Default","deleted_at":null},"assigned_to_user":{"id":"P9L426X","name":"Yali Sassoon","email":"yali@snowplowanalytics.com","html_url":"https://snowplow.pagerduty.com/users/P9L426X"},"trigger_summary_data":{"description":"FAILURE for production/HTTP on machine srv01.acme.com","client":"Sample Monitoring Service","client_url":"https://monitoring.service.com"},"trigger_details_html_url":"https://snowplow.pagerduty.com/incidents/P9WY9U9/log_entries/P5AWPTR","trigger_type":"trigger_svc_event","last_status_change_on":"2014-11-12T18:53:47Z","last_status_change_by":null,"number_of_escalations":0,"assigned_to":[{"at":"2014-11-12T18:53:47Z","object":{"id":"P9L426X","name":"Yali Sassoon","email":"yali@snowplowanalytics.com","html_url":"https://snowplow.pagerduty.com/users/P9L426X","type":"user"}}]}},"id":"3c3e8ee0-6a9d-11e4-b3d5-22000ae31361","created_on":"2014-11-12T18:53:47Z"}]}"""
    val payload = CollectorPayload(Shared.api, Nil, ContentType.some, bodyStr.some, Shared.cljSource, Shared.context)
    val expected = NonEmptyList(RawEvent(Shared.api,Map("tv" -> "com.pagerduty-v1", "e" -> "ue", "p" -> "srv", "ue_pr" -> """{"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0","data":{"schema":"iglu:com.pagerduty/incident/jsonschema/1-0-0","data":{"type":"trigger","data":{"incident":{"id":"P9WY9U9","incident_number":139,"created_on":"2014-11-12T18:53:47+00:00","status":"triggered","html_url":"https://snowplow.pagerduty.com/incidents/P9WY9U9","incident_key":"srv01/HTTP","service":{"id":"PTHO4FF","name":"Webhooks Test","html_url":"https://snowplow.pagerduty.com/services/PTHO4FF","deleted_at":null},"escalation_policy":{"id":"P8ETVHU","name":"Default","deleted_at":null},"assigned_to_user":{"id":"P9L426X","name":"Yali Sassoon","email":"yali@snowplowanalytics.com","html_url":"https://snowplow.pagerduty.com/users/P9L426X"},"trigger_summary_data":{"description":"FAILURE for production/HTTP on machine srv01.acme.com","client":"Sample Monitoring Service","client_url":"https://monitoring.service.com"},"trigger_details_html_url":"https://snowplow.pagerduty.com/incidents/P9WY9U9/log_entries/P5AWPTR","trigger_type":"trigger_svc_event","last_status_change_on":"2014-11-12T18:53:47Z","last_status_change_by":null,"number_of_escalations":0,"assigned_to":[{"at":"2014-11-12T18:53:47Z","object":{"id":"P9L426X","name":"Yali Sassoon","email":"yali@snowplowanalytics.com","html_url":"https://snowplow.pagerduty.com/users/P9L426X","type":"user"}}]}},"id":"3c3e8ee0-6a9d-11e4-b3d5-22000ae31361","created_on":"2014-11-12T18:53:47Z"}}}"""),ContentType.some, Shared.cljSource, Shared.context))
    PagerdutyAdapter.toRawEvents(payload) must beSuccessful(expected)
  }

  def e7 = {
    val bodyStr = """{"messages":[{"type":"trigger","data":{"incident":{"id":"P9WY9U9","incident_number":139,"created_on":"2014-11-12T18:53:47 00:00","status":"triggered","html_url":"https://snowplow.pagerduty.com/incidents/P9WY9U9","incident_key":"srv01/HTTP","service":{"id":"PTHO4FF","name":"Webhooks Test","html_url":"https://snowplow.pagerduty.com/services/PTHO4FF","deleted_at":null},"escalation_policy":{"id":"P8ETVHU","name":"Default","deleted_at":null},"assigned_to_user":{"id":"P9L426X","name":"Yali Sassoon","email":"yali@snowplowanalytics.com","html_url":"https://snowplow.pagerduty.com/users/P9L426X"},"trigger_summary_data":{"description":"FAILURE for production/HTTP on machine srv01.acme.com","client":"Sample Monitoring Service","client_url":"https://monitoring.service.com"},"trigger_details_html_url":"https://snowplow.pagerduty.com/incidents/P9WY9U9/log_entries/P5AWPTR","trigger_type":"trigger_svc_event","last_status_change_on":"2014-11-12T18:53:47Z","last_status_change_by":null,"number_of_escalations":0,"assigned_to":[{"at":"2014-11-12T18:53:47Z","object":{"id":"P9L426X","name":"Yali Sassoon","email":"yali@snowplowanalytics.com","html_url":"https://snowplow.pagerduty.com/users/P9L426X","type":"user"}}]}},"id":"3c3e8ee0-6a9d-11e4-b3d5-22000ae31361","created_on":"2014-11-12T18:53:47Z"}]}"""
    val payload = CollectorPayload(Shared.api, Nil, ContentType.some, bodyStr.some, Shared.cljSource, Shared.context)
    val expected = "PagerDuty event at index [0] failed: type parameter [trigger] not recognized"
    PagerdutyAdapter.toRawEvents(payload) must beFailing(NonEmptyList(expected))
  }

  def e8 = {
    val payload = CollectorPayload(Shared.api, Nil, ContentType.some, None, Shared.cljSource, Shared.context)
    PagerdutyAdapter.toRawEvents(payload) must beFailing(NonEmptyList("Request body is empty: no PagerDuty events to process"))
  }

  def e9 = {
    val payload = CollectorPayload(Shared.api, Nil, None, "stub".some, Shared.cljSource, Shared.context)
    PagerdutyAdapter.toRawEvents(payload) must beFailing(NonEmptyList("Request body provided but content type empty, expected application/json for PagerDuty"))
  }

  def e10 = {
    val payload = CollectorPayload(Shared.api, Nil, "application/x-www-form-urlencoded".some, "stub".some, Shared.cljSource, Shared.context)
    PagerdutyAdapter.toRawEvents(payload) must beFailing(NonEmptyList("Content type of application/x-www-form-urlencoded provided, expected application/json for PagerDuty"))
  }
}
