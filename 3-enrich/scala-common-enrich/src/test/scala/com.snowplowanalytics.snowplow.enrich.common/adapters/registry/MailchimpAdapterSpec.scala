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

  "This is a specification to test the MailchimpAdapter functionality"                                                ^
                                                                                                                     p^
  "toKeys should return a valid List of Keys from a string containing braces (or not)"                                ! e1^
  "recurse should return a valid JObject which contains the toKeys list and value supplied"                           ! e2^
  "getJsonObject should return a valid list JObjects which pertain to the map supplied"                               ! e3^
  "mergeJObjects should return a correctly merged JSON which matches the expectation"                                 ! e4^
  "mergeJObjects should return a Nel Failure for an empty List of JObjects"                                           ! e5^
  "getSchema should return the correct schema for a valid event type"                                                 ! e6^
  "getSchema should return a Nel Failure error for a bad event type"                                                  ! e7^
  "toRawEvents should return a Nel Success containing an unsubscribe event and query string parameters"               ! e8^
  "toRawEvents must be failing if no type parameter is passed in the body"                                            ! e9^
  "toRawEvents must be failing if the body content is empty"                                                          ! e10^
                                                                                                                     end
  implicit val resolver = SpecHelpers.IgluResolver

  object Shared {
    val api = CollectorApi("com.mailchimp", "v1")
    val cfSource = CollectorSource("cloudfront", "UTF-8", None)
    val cljSource = CollectorSource("clj-tomcat", "UTF-8", None)
    val context = CollectorContext(DateTime.parse("2013-08-29T00:18:48.000+00:00"), "37.157.33.123".some, None, None, Nil, None)
  }

  object Expected {
    val staticNoPlatform = Map(
      "tv" -> "com.mailchimp-v1",
      "e"  -> "ue"
      )
    val static = staticNoPlatform ++ Map(
      "p"  -> "app"
    )
  }

  val ContentType = "application/x-www-form-urlencoded; charset=utf-8"

  def e1 = {
    val toKeysTest = MailchimpAdapter.toKeys("data[merges][LNAME]")
    val expected = List("data","merges","LNAME")
    toKeysTest mustEqual expected
  }

  def e2 = {
    val keysArray = List("data","merges","LNAME")
    val value = "Beemster"
    val expected = JObject(List(("data",JObject(List(("merges",JObject(List(("LNAME",JString("Beemster"))))))))))
    val testRecursive = MailchimpAdapter.recurse(keysArray, value)
    testRecursive mustEqual expected
  }

  def e3 = {
    val m = Map("data[merges][LNAME]" -> "Beemster")
    val expected = List(JObject(List(("data",JObject(List(("merges",JObject(List(("LNAME",JString("Beemster")))))))))))
    val testMap = MailchimpAdapter.getJsonObject(m)
    testMap mustEqual expected
  }

  def e4 = {
    val m = Map("data[merges][LNAME]" -> "Beemster", "data[merges][FNAME]" -> "Joshua")
    val jsonObject = MailchimpAdapter.getJsonObject(m)
    val actual = MailchimpAdapter.mergeJObjects(jsonObject)
    val expected = JObject(List(("data",JObject(List(("merges",JObject(List(("LNAME",JString("Beemster")), ("FNAME",JString("Joshua"))))))))))
    actual must beSuccessful(expected)
  }

  def e5 = {
    val actual = MailchimpAdapter.mergeJObjects(List())
    actual must beFailing(NonEmptyList("No JObjects to merge"))
  }

  def e6 = 
    "SPEC NAME"               || "SCHEMA TYPE"  | "EXPECTED OUTPUT"                                               |
    "Valid, type subscribe"   !! "subscribe"    ! "iglu:com.mailchimp/subscribe/jsonschema/1-0-0"                 |
    "Valid, type unsubscribe" !! "unsubscribe"  ! "iglu:com.mailchimp/unsubscribe/jsonschema/1-0-0"               |
    "Valid, type profile"     !! "profile"      ! "iglu:com.mailchimp/profile_update/jsonschema/1-0-0"            |
    "Valid, type email"       !! "upemail"      ! "iglu:com.mailchimp/email_address_change/jsonschema/1-0-0"      |
    "Valid, type cleaned"     !! "cleaned"      ! "iglu:com.mailchimp/cleaned_email/jsonschema/1-0-0"             |
    "Valid, type campaign"    !! "campaign"     ! "iglu:com.mailchimp/campaign_sending_status/jsonschema/1-0-0"   |> {
      (_, schema, expected) => MailchimpAdapter.getSchema(Some(schema)) must beSuccessful(expected)
  }

  def e7 = 
    "SPEC NAME"               || "SCHEMA TYPE"  | "EXPECTED OUTPUT"                                               |
    "Invalid, bad type"       !! Some("bad")    ! "Invalid event type passed to getSchema - bad"                  |
    "Invalid, no type"        !! None           ! "No event type passed to getSchema"                             |> {
      (_, schema, expected) => MailchimpAdapter.getSchema(schema) must beFailing(NonEmptyList(expected))
  }

  def e8 = {
    val body = "type=unsubscribe&fired_at=2014-10-22+13%3A10%3A40&data%5Baction%5D=unsub&data%5Breason%5D=manual&data%5Bid%5D=94826aa750&data%5Bemail%5D=josh%40snowplowanalytics.com&data%5Bemail_type%5D=html&data%5Bip_opt%5D=82.225.169.220&data%5Bweb_id%5D=203740265&data%5Bmerges%5D%5BEMAIL%5D=josh%40snowplowanalytics.com&data%5Bmerges%5D%5BFNAME%5D=Joshua&data%5Bmerges%5D%5BLNAME%5D=Beemster&data%5Blist_id%5D=f1243a3b12"
    val qs = toNameValuePairs("nuid" -> "123")
    val payload = CollectorPayload(Shared.api, qs, ContentType.some, body.some, Shared.cfSource, Shared.context)

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
                |"fired_at":"2014-10-22 13:10:40",
                |"type":"unsubscribe"
              |}
            |}
          |}""".stripMargin.replaceAll("[\n\r]","")
    val actual = MailchimpAdapter.toRawEvents(payload)

    actual must beSuccessful(NonEmptyList(RawEvent(Shared.api, Map("tv" -> "com.mailchimp-v1", "e" -> "ue", "p" -> "app", "ue_pr" -> expectedJson), ContentType.some, Shared.cfSource, Shared.context)))
  }

  def e9 = {
    val body = "fired_at=2014-10-22+13%3A10%3A40"
    val payload = CollectorPayload(Shared.api, Nil, ContentType.some, body.some, Shared.cfSource, Shared.context)
    val actual = MailchimpAdapter.toRawEvents(payload)

    actual must beFailing(NonEmptyList("No event type passed with event body"))
  }

  def e10 = {
    val body = ""
    val payload = CollectorPayload(Shared.api, Nil, ContentType.some, body.some, Shared.cfSource, Shared.context)
    val actual = MailchimpAdapter.toRawEvents(payload)

    actual must beFailing(NonEmptyList("Mailchimp Events require information to be in the body"))
  }
}
