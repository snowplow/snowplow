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

class MailchimpAdapterSpec extends Specification with DataTables with ValidationMatchers with ScalaCheck { def is =

  "This is a specification to test the MailchimpAdapter functionality"                                              ^
                                                                                                                   p^
  "toKeys should return a valid List of Keys from a string containing braces (or not)"                              ! e1^
  "toNestedJField should return a valid JField nested to contain all keys and then the supplied value"              ! e2^
  "toJFields should return a valid list of JFields based on the Map supplied"                                       ! e3^
  "mergeJFields should return a correctly merged JSON which matches the expectation"                                ! e4^
  "reformatParameters should return a parameter Map with correctly formatted values"                                ! e5^
  "toRawEvents must return a Nel Success with a correctly formatted ue_pr json"                                     ! e6^
  "toRawEvents must return a Nel Success with a correctly merged and formatted ue_pr json"                          ! e7^
  "toRawEvents must return a Nel Success for a supported event type"                                                ! e8^
  "toRawEvents must return a Nel Failure error for an unsupported event type"                                       ! e9^
  "toRawEvents must return a Nel Success containing an unsubscribe event and query string parameters"               ! e10^
  "toRawEvents must return a Nel Failure if the request body is missing"                                            ! e11^
  "toRawEvents must return a Nel Failure if the content type is missing"                                            ! e12^
  "toRawEvents must return a Nel Failure if the content type is incorrect"                                          ! e13^
  "toRawEvents must return a Nel Failure if the request body does not contain a type parameter"                     ! e14^
                                                                                                                    end
  implicit val resolver = SpecHelpers.IgluResolver

  object Shared {
    val api = CollectorApi("com.mailchimp", "v1")
    val cljSource = CollectorSource("clj-tomcat", "UTF-8", None)
    val context = CollectorContext(DateTime.parse("2013-08-29T00:18:48.000+00:00").some, "37.157.33.123".some, None, None, Nil, None)
  }

  val ContentType = "application/x-www-form-urlencoded"

  def e1 = {
    val keys = "data[merges][LNAME]"
    val expected = NonEmptyList("data", "merges", "LNAME")

    MailchimpAdapter.toKeys(keys) mustEqual expected
  }

  def e2 = {
    val keys = NonEmptyList("data", "merges", "LNAME")
    val value = "Beemster"
    val expected = JField("data", JObject(List(("merges", JObject(List(("LNAME", JString("Beemster"))))))))

    MailchimpAdapter.toNestedJField(keys, value) mustEqual expected
  }

  def e3 = {
    val map = Map(
      "data[merges][LNAME]" -> "Beemster",
      "data[merges][FNAME]" -> "Joshua"
    )
    val expected = List(
      JField("data", JObject(List(("merges", JObject(List(("LNAME", JString("Beemster")))))))),
      JField("data", JObject(List(("merges", JObject(List(("FNAME", JString("Joshua"))))))))
    )

    MailchimpAdapter.toJFields(map) mustEqual expected
  }

  def e4 = {
    val a = JField("l1", JField("l2", JField("l3", JField("str", "hi"))))
    val b = JField("l1", JField("l2", JField("l3", JField("num", 42))))
    val expected = JObject(List(("l1", JObject(List(("l2", JObject(List(("l3", JObject(List(
      ("str", JString("hi")),
      ("num", JInt(42))
    )))))))))))

    MailchimpAdapter.mergeJFields(List(a, b)) mustEqual expected
  }

  def e5 =
    "SPEC NAME"             || "PARAMS"                                                        | "EXPECTED OUTPUT"                                                    |
    "Return Updated Params" !! Map("type" -> "subscribe", "fired_at" -> "2014-10-22 13:50:00") ! Map("type" -> "subscribe", "fired_at" -> "2014-10-22T13:50:00.000Z") |
    "Return Same Params"    !! Map("type" -> "subscribe", "id" -> "some_id")                   ! Map("type" -> "subscribe", "id" -> "some_id")                        |> {
      (_, params, expected) => 
      val actual = MailchimpAdapter.reformatParameters(params)
      actual mustEqual expected
  }

  def e6 = {
    val body = "type=subscribe&data%5Bmerges%5D%5BLNAME%5D=Beemster"
    val payload = CollectorPayload(Shared.api, Nil, ContentType.some, body.some, Shared.cljSource, Shared.context)
    val expectedJson = 
      """|{
            |"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0",
            |"data":{
              |"schema":"iglu:com.mailchimp/subscribe/jsonschema/1-0-0",
              |"data":{
                |"type":"subscribe",
                |"data":{
                  |"merges":{
                    |"LNAME":"Beemster"
                  |}
                |}
              |}
            |}
          |}""".stripMargin.replaceAll("[\n\r]","")

    val actual = MailchimpAdapter.toRawEvents(payload)
    actual must beSuccessful(NonEmptyList(RawEvent(Shared.api, Map("tv" -> "com.mailchimp-v1", "e" -> "ue", "p" -> "srv", "ue_pr" -> expectedJson), ContentType.some, Shared.cljSource, Shared.context)))
  }

  def e7 = {
    val body = "type=subscribe&data%5Bmerges%5D%5BFNAME%5D=Agent&data%5Bmerges%5D%5BLNAME%5D=Smith"
    val payload = CollectorPayload(Shared.api, Nil, ContentType.some, body.some, Shared.cljSource, Shared.context)
    val expectedJson = 
      """|{
            |"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0",
            |"data":{
              |"schema":"iglu:com.mailchimp/subscribe/jsonschema/1-0-0",
              |"data":{
                |"type":"subscribe",
                |"data":{
                  |"merges":{
                    |"FNAME":"Agent",
                    |"LNAME":"Smith"
                  |}
                |}
              |}
            |}
          |}""".stripMargin.replaceAll("[\n\r]","")

    val actual = MailchimpAdapter.toRawEvents(payload)
    actual must beSuccessful(NonEmptyList(RawEvent(Shared.api, Map("tv" -> "com.mailchimp-v1", "e" -> "ue", "p" -> "srv", "ue_pr" -> expectedJson), ContentType.some, Shared.cljSource, Shared.context)))
  }

  def e8 = 
    "SPEC NAME"               || "SCHEMA TYPE"  | "EXPECTED SCHEMA"                                               |
    "Valid, type subscribe"   !! "subscribe"    ! "iglu:com.mailchimp/subscribe/jsonschema/1-0-0"                 |
    "Valid, type unsubscribe" !! "unsubscribe"  ! "iglu:com.mailchimp/unsubscribe/jsonschema/1-0-0"               |
    "Valid, type profile"     !! "profile"      ! "iglu:com.mailchimp/profile_update/jsonschema/1-0-0"            |
    "Valid, type email"       !! "upemail"      ! "iglu:com.mailchimp/email_address_change/jsonschema/1-0-0"      |
    "Valid, type cleaned"     !! "cleaned"      ! "iglu:com.mailchimp/cleaned_email/jsonschema/1-0-0"             |
    "Valid, type campaign"    !! "campaign"     ! "iglu:com.mailchimp/campaign_sending_status/jsonschema/1-0-0"   |> {
      (_, schema, expected) => 
        val body = "type="+schema
        val payload = CollectorPayload(Shared.api, Nil, ContentType.some, body.some, Shared.cljSource, Shared.context)
        val expectedJson = "{\"schema\":\"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0\",\"data\":{\"schema\":\""+expected+"\",\"data\":{\"type\":\""+schema+"\"}}}"
        val actual = MailchimpAdapter.toRawEvents(payload)
        actual must beSuccessful(NonEmptyList(RawEvent(Shared.api, Map("tv" -> "com.mailchimp-v1", "e" -> "ue", "p" -> "srv", "ue_pr" -> expectedJson), ContentType.some, Shared.cljSource, Shared.context)))
  }

  def e9 = 
    "SPEC NAME"               || "SCHEMA TYPE"  | "EXPECTED OUTPUT"                                                               |
    "Invalid, bad type"       !! "bad"          ! "MailChimp event failed: type parameter [bad] not recognized"                   |
    "Invalid, no type"        !! ""             ! "MailChimp event failed: type parameter is empty - cannot determine event type" |> {
      (_, schema, expected) =>
        val body = "type="+schema
        val payload = CollectorPayload(Shared.api, Nil, ContentType.some, body.some, Shared.cljSource, Shared.context)
        val actual = MailchimpAdapter.toRawEvents(payload)
        actual must beFailing(NonEmptyList(expected))
  }

  def e10 = {
    val body = "type=unsubscribe&fired_at=2014-10-22+13%3A10%3A40&data%5Baction%5D=unsub&data%5Breason%5D=manual&data%5Bid%5D=94826aa750&data%5Bemail%5D=josh%40snowplowanalytics.com&data%5Bemail_type%5D=html&data%5Bip_opt%5D=82.225.169.220&data%5Bweb_id%5D=203740265&data%5Bmerges%5D%5BEMAIL%5D=josh%40snowplowanalytics.com&data%5Bmerges%5D%5BFNAME%5D=Joshua&data%5Bmerges%5D%5BLNAME%5D=Beemster&data%5Blist_id%5D=f1243a3b12"
    val qs = toNameValuePairs("nuid" -> "123")
    val payload = CollectorPayload(Shared.api, qs, ContentType.some, body.some, Shared.cljSource, Shared.context)
    val expectedJson = 
      """|{
            |"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0",
            |"data":{
              |"schema":"iglu:com.mailchimp/unsubscribe/jsonschema/1-0-0",
              |"data":{
                |"data":{
                  |"ip_opt":"82.225.169.220",
                  |"merges":{
                    |"LNAME":"Beemster",
                    |"FNAME":"Joshua",
                    |"EMAIL":"josh@snowplowanalytics.com"
                  |},
                  |"email":"josh@snowplowanalytics.com",
                  |"list_id":"f1243a3b12",
                  |"email_type":"html",
                  |"reason":"manual",
                  |"id":"94826aa750",
                  |"action":"unsub",
                  |"web_id":"203740265"
                |},
                |"fired_at":"2014-10-22T13:10:40.000Z",
                |"type":"unsubscribe"
              |}
            |}
          |}""".stripMargin.replaceAll("[\n\r]","")

    val actual = MailchimpAdapter.toRawEvents(payload)
    actual must beSuccessful(NonEmptyList(RawEvent(Shared.api, Map("tv" -> "com.mailchimp-v1", "e" -> "ue", "p" -> "srv", "ue_pr" -> expectedJson, "nuid" -> "123"), ContentType.some, Shared.cljSource, Shared.context)))
  }

  def e11 = {
    val payload = CollectorPayload(Shared.api, Nil, ContentType.some, None, Shared.cljSource, Shared.context)
    val actual = MailchimpAdapter.toRawEvents(payload)
    actual must beFailing(NonEmptyList("Request body is empty: no MailChimp event to process"))
  }

  def e12 = {
    val payload = CollectorPayload(Shared.api, Nil, None, "stub".some, Shared.cljSource, Shared.context)
    val actual = MailchimpAdapter.toRawEvents(payload)
    actual must beFailing(NonEmptyList("Request body provided but content type empty, expected application/x-www-form-urlencoded for MailChimp"))
  }

  def e13 = {
    val payload = CollectorPayload(Shared.api, Nil, "application/json".some, "stub".some, Shared.cljSource, Shared.context)
    val actual = MailchimpAdapter.toRawEvents(payload)
    actual must beFailing(NonEmptyList("Content type of application/json provided, expected application/x-www-form-urlencoded for MailChimp"))
  }

  def e14 = {
    val body = "fired_at=2014-10-22+13%3A10%3A40"
    val payload = CollectorPayload(Shared.api, Nil, ContentType.some, body.some, Shared.cljSource, Shared.context)
    val actual = MailchimpAdapter.toRawEvents(payload)
    actual must beFailing(NonEmptyList("No MailChimp type parameter provided: cannot determine event type"))
  }
}
