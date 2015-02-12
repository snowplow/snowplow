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

class PingdomAdapterSpec extends Specification with DataTables with ValidationMatchers with ScalaCheck { def is =

  "This is a specification to test the PingdomAdapter functionality"                                                ^
                                                                                                                   p^
  "reformatParameters should return either an updated JSON without the 'action' field or the same JSON"           ! e1^
  "parseJson must return a Success Nel for a valid json string being passed"                                      ! e2^
  "parseJson must return a Failure Nel containing the JsonParseException for invalid json strings"                ! e3^
  "reformatMapParams must return a Failure Nel for any Python Unicode wrapped values"                             ! e4^
  "toRawEvents must return a Success Nel for a valid querystring"                                                 ! e5^
  "toRawEvents must return a Failure Nel for an empty querystring"                                                ! e6^
  "toRawEvents must return a Failure Nel for a querystring which does not contain 'message' as a key"             ! e7^
                                                                                                                   end

  implicit val resolver = SpecHelpers.IgluResolver

  object Shared {
    val api = CollectorApi("com.pingdom", "v1")
    val cljSource = CollectorSource("clj-tomcat", "UTF-8", None)
    val context = CollectorContext(DateTime.parse("2013-08-29T00:18:48.000+00:00").some, "37.157.33.123".some, None, None, Nil, None)
  }

  def e1 = 
    "SPEC NAME"           || "JSON"                                     | "EXPECTED OUTPUT"                          |
    "Remove action field" !! """{"action":"assign","agent":"smith"}"""  ! """{"agent":"smith"}"""                    |
    "Nothing removed"     !! """{"actions":"assign","agent":"smith"}""" ! """{"actions":"assign","agent":"smith"}""" |>{
      (_, json, expected) => PingdomAdapter.reformatParameters(parse(json)) mustEqual parse(expected)
  }

  def e2 = {
    val jsonStr = """{"event":"incident_assign"}"""
    val expected = JObject(List(("event",JString("incident_assign"))))
    PingdomAdapter.parseJson(jsonStr) must beSuccessful(expected)
  }

  def e3 = {
    val jsonStr = """{"event":incident_assign"}"""
    val expected = "Pingdom event failed to parse into JSON: [com.fasterxml.jackson.core.JsonParseException: Unrecognized token 'incident_assign': was expecting ('true', 'false' or 'null') at [Source: java.io.StringReader@xxxxxx; line: 1, column: 25]]"
    PingdomAdapter.parseJson(jsonStr) must beFailing(NonEmptyList(expected))
  }

  def e4 = {
    val nvPairs = toNameValuePairs("p" -> "(u'apps',)")
    val expected = "Pingdom name-value pair [p -> apps]: Passed regex - Collector is not catching unicode wrappers anymore"
    PingdomAdapter.reformatMapParams(nvPairs) must beFailing(NonEmptyList(expected))
  }

  def e5 = {
    val querystring = toNameValuePairs("p" -> "apps", "message" -> """{"check": "1421338", "checkname": "Webhooks_Test", "host": "7eef51c2.ngrok.com", "action": "assign", "incidentid": 3, "description": "down"}""")
    val payload = CollectorPayload(Shared.api, querystring, None, None, Shared.cljSource, Shared.context)
    val expected = RawEvent(Shared.api,Map("tv" -> "com.pingdom-v1", "e" -> "ue", "p" -> "apps", "ue_pr" -> """{"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0","data":{"schema":"iglu:com.pingdom/incident_assign/jsonschema/1-0-0","data":{"check":"1421338","checkname":"Webhooks_Test","host":"7eef51c2.ngrok.com","incidentid":3,"description":"down"}}}"""),None,Shared.cljSource,Shared.context)
    PingdomAdapter.toRawEvents(payload) must beSuccessful(NonEmptyList(expected))
  }

  def e6 = {
    val payload = CollectorPayload(Shared.api, Nil, None, None, Shared.cljSource, Shared.context)
    val expected = "Pingdom payload querystring is empty: nothing to process"
    PingdomAdapter.toRawEvents(payload) must beFailing(NonEmptyList(expected))
  }

  def e7 = {
    val querystring = toNameValuePairs("p" -> "apps")
    val payload = CollectorPayload(Shared.api, querystring, None, None, Shared.cljSource, Shared.context)
    val expected = "Pingdom payload querystring does not have 'message' as a key: no event to process"
    PingdomAdapter.toRawEvents(payload) must beFailing(NonEmptyList(expected))
  }
}
