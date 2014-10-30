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

  "This is a specification to test the MailchimpAdapter functionality"                                                       ^
                                                                                                                            p^
  "toKeys should return a valid List of Keys from a string containing braces (or not)"                                       ! e1^
  "recurse should return a valid JObject which contains the toKeys list and value supplied"                                  ! e2^
  "getJsonObject should return a valid list JObjects which pertain to the map supplied"                                      ! e3^
  "mergeJObjects should return a correctly merged JSON which matches the expectation"                                        ! e4^
  "getSchema should return the correct schema for the subscribe event type"                                                  ! e5^
  "getSchema should return the correct schema for the unsubscribe event type"                                                ! e6^
  "getSchema should return the correct schema for the campaign event type"                                                   ! e7^
  "getSchema should return the correct schema for the cleaned event type"                                                    ! e8^
  "getSchema should return the correct schema for the upemail event type"                                                    ! e9^
  "getSchema should return the correct schema for the profile event type"                                                    ! e10^
  "getSchema should return a runtime error for a bad event type"                                                             ! e11^
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
    val mergedJsonString = compact(render(MailchimpAdapter.mergeJObjects(jsonObject)))
    val expected = "{\"data\":{\"merges\":{\"LNAME\":\"Beemster\",\"FNAME\":\"Joshua\"}}}"
    mergedJsonString mustEqual expected
  }

  def e5 = {
    val schemaType = "subscribe"
    val schemaReturn = MailchimpAdapter.getSchema(Some(schemaType))
    val expected = "iglu:com.mailchimp/subscribe/jsonschema/1-0-0"
    schemaReturn mustEqual expected
  }

  def e6 = {
    val schemaType = "unsubscribe"
    val schemaReturn = MailchimpAdapter.getSchema(Some(schemaType))
    val expected = "iglu:com.mailchimp/unsubscribe/jsonschema/1-0-0"
    schemaReturn mustEqual expected
  }

  def e7 = {
    val schemaType = "campaign"
    val schemaReturn = MailchimpAdapter.getSchema(Some(schemaType))
    val expected = "iglu:com.mailchimp/campaign_sending_status/jsonschema/1-0-0"
    schemaReturn mustEqual expected
  }

  def e8 = {
    val schemaType = "cleaned"
    val schemaReturn = MailchimpAdapter.getSchema(Some(schemaType))
    val expected = "iglu:com.mailchimp/cleaned_email/jsonschema/1-0-0"
    schemaReturn mustEqual expected
  }

  def e9 = {
    val schemaType = "upemail"
    val schemaReturn = MailchimpAdapter.getSchema(Some(schemaType))
    val expected = "iglu:com.mailchimp/email_address_change/jsonschema/1-0-0"
    schemaReturn mustEqual expected
  }

  def e10 = {
    val schemaType = "profile"
    val schemaReturn = MailchimpAdapter.getSchema(Some(schemaType))
    val expected = "iglu:com.mailchimp/profile_update/jsonschema/1-0-0"
    schemaReturn mustEqual expected
  }

  def e11 = {
    (MailchimpAdapter.getSchema(Some("")) must throwA[RuntimeException]).message mustEqual 
      "Got the exception java.lang.RuntimeException: Invalid Event Type specified."
  }
}
