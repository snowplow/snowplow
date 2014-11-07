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

class MandrillAdapterSpec extends Specification with DataTables with ValidationMatchers with ScalaCheck { def is =

  "This is a specification to test the MailchimpAdapter functionality"                                                ^
                                                                                                                     p^
  "extractKeyValueFromJson should return an Option[String] or None if it can or cannot find a valid value"          ! e1^
  "extractKeyValueFromJson should return an Option[String] or None if it can or cannot find a valid value"          ! e2^
  "extractKeyValueFromJson should return an Option[String] or None if it can or cannot find a valid value"          ! e3^
  "extractKeyValueFromJson should return an Option[String] or None if it can or cannot find a valid value"          ! e4^
                                                                                                                     end
  implicit val resolver = SpecHelpers.IgluResolver

  object Shared {
    val api = CollectorApi("com.mandrill", "v1")
    val cljSource = CollectorSource("clj-tomcat", "UTF-8", None)
    val context = CollectorContext(DateTime.parse("2013-08-29T00:18:48.000+00:00"), "37.157.33.123".some, None, None, Nil, None)
  }

  val ContentType = "application/x-www-form-urlencoded; charset=utf-8"

  def e1 = 
    "SPEC NAME"               || "KEY"   | "JSON"                             | "EXPECTED OUTPUT"  |
    "Valid, value found"      !! "event" ! parse("""{"event":"subscribe"}""") ! Some("subscribe")  |
    "Invalid, no value found" !! "none"  ! parse("""{"event":"subscribe"}""") ! None               |> {
      (_, key, json, expected) => MandrillAdapter.extractKeyValueFromJson(key,json) mustEqual expected
  }

  def e2 = {
    val payload = CollectorPayload(Shared.api, Nil, ContentType.some, None, Shared.cljSource, Shared.context)
    val actual = MandrillAdapter.toRawEvents(payload)
    actual must beFailing(NonEmptyList("Request body is empty: no Mandrill events to process"))
  }

  def e3 = {
    val body = "doesnotstartwithmandrill_events="
    val payload = CollectorPayload(Shared.api, Nil, ContentType.some, body.some, Shared.cljSource, Shared.context)
    val actual = MandrillAdapter.toRawEvents(payload)
    actual must beFailing(NonEmptyList("Request body is formatted incorrectly: cannot process"))
  }

  def e4 = {
    val body = "mandrill_events=%5B%7B%22event%22%3A%22send%22%2C%22msg%22%3A%7B%22ts%22%3A1365109999%2C%22subject%22%3A%22This+an+example+webhook+message%22%2C%22email%22%3A%22example.webhook%40mandrillapp.com%22%2C%22sender%22%3A%22example.sender%40mandrillapp.com%22%2C%22tags%22%3A%5B%22webhook-example%22%5D%2C%22opens%22%3A%5B%5D%2C%22clicks%22%3A%5B%5D%2C%22state%22%3A%22sent%22%2C%22metadata%22%3A%7B%22user_id%22%3A111%7D%2C%22_id%22%3A%22exampleaaaaaaaaaaaaaaaaaaaaaaaaa%22%2C%22_version%22%3A%22exampleaaaaaaaaaaaaaaa%22%7D%2C%22_id%22%3A%22exampleaaaaaaaaaaaaaaaaaaaaaaaaa%22%2C%22ts%22%3A1415267366%7D%2C%7B%22event%22%3A%22deferral%22%2C%22msg%22%3A%7B%22ts%22%3A1365109999%2C%22subject%22%3A%22This+an+example+webhook+message%22%2C%22email%22%3A%22example.webhook%40mandrillapp.com%22%2C%22sender%22%3A%22example.sender%40mandrillapp.com%22%2C%22tags%22%3A%5B%22webhook-example%22%5D%2C%22opens%22%3A%5B%5D%2C%22clicks%22%3A%5B%5D%2C%22state%22%3A%22deferred%22%2C%22metadata%22%3A%7B%22user_id%22%3A111%7D%2C%22_id%22%3A%22exampleaaaaaaaaaaaaaaaaaaaaaaaaa1%22%2C%22_version%22%3A%22exampleaaaaaaaaaaaaaaa%22%2C%22smtp_events%22%3A%5B%7B%22destination_ip%22%3A%22127.0.0.1%22%2C%22diag%22%3A%22451+4.3.5+Temporarily+unavailable%2C+try+again+later.%22%2C%22source_ip%22%3A%22127.0.0.1%22%2C%22ts%22%3A1365111111%2C%22type%22%3A%22deferred%22%2C%22size%22%3A0%7D%5D%7D%2C%22_id%22%3A%22exampleaaaaaaaaaaaaaaaaaaaaaaaaa1%22%2C%22ts%22%3A1415267366%7D%2C%7B%22event%22%3A%22hard_bounce%22%2C%22msg%22%3A%7B%22ts%22%3A1365109999%2C%22subject%22%3A%22This+an+example+webhook+message%22%2C%22email%22%3A%22example.webhook%40mandrillapp.com%22%2C%22sender%22%3A%22example.sender%40mandrillapp.com%22%2C%22tags%22%3A%5B%22webhook-example%22%5D%2C%22state%22%3A%22bounced%22%2C%22metadata%22%3A%7B%22user_id%22%3A111%7D%2C%22_id%22%3A%22exampleaaaaaaaaaaaaaaaaaaaaaaaaa2%22%2C%22_version%22%3A%22exampleaaaaaaaaaaaaaaa%22%2C%22bounce_description%22%3A%22bad_mailbox%22%2C%22bgtools_code%22%3A10%2C%22diag%22%3A%22smtp%3B550+5.1.1+The+email+account+that+you+tried+to+reach+does+not+exist.+Please+try+double-checking+the+recipient%27s+email+address+for+typos+or+unnecessary+spaces.%22%7D%2C%22_id%22%3A%22exampleaaaaaaaaaaaaaaaaaaaaaaaaa2%22%2C%22ts%22%3A1415267366%7D%2C%7B%22event%22%3A%22soft_bounce%22%2C%22msg%22%3A%7B%22ts%22%3A1365109999%2C%22subject%22%3A%22This+an+example+webhook+message%22%2C%22email%22%3A%22example.webhook%40mandrillapp.com%22%2C%22sender%22%3A%22example.sender%40mandrillapp.com%22%2C%22tags%22%3A%5B%22webhook-example%22%5D%2C%22state%22%3A%22soft-bounced%22%2C%22metadata%22%3A%7B%22user_id%22%3A111%7D%2C%22_id%22%3A%22exampleaaaaaaaaaaaaaaaaaaaaaaaaa3%22%2C%22_version%22%3A%22exampleaaaaaaaaaaaaaaa%22%2C%22bounce_description%22%3A%22mailbox_full%22%2C%22bgtools_code%22%3A22%2C%22diag%22%3A%22smtp%3B552+5.2.2+Over+Quota%22%7D%2C%22_id%22%3A%22exampleaaaaaaaaaaaaaaaaaaaaaaaaa3%22%2C%22ts%22%3A1415267366%7D%2C%7B%22event%22%3A%22open%22%2C%22msg%22%3A%7B%22ts%22%3A1365109999%2C%22subject%22%3A%22This+an+example+webhook+message%22%2C%22email%22%3A%22example.webhook%40mandrillapp.com%22%2C%22sender%22%3A%22example.sender%40mandrillapp.com%22%2C%22tags%22%3A%5B%22webhook-example%22%5D%2C%22opens%22%3A%5B%7B%22ts%22%3A1365111111%7D%5D%2C%22clicks%22%3A%5B%7B%22ts%22%3A1365111111%2C%22url%22%3A%22http%3A%5C%2F%5C%2Fmandrill.com%22%7D%5D%2C%22state%22%3A%22sent%22%2C%22metadata%22%3A%7B%22user_id%22%3A111%7D%2C%22_id%22%3A%22exampleaaaaaaaaaaaaaaaaaaaaaaaaa4%22%2C%22_version%22%3A%22exampleaaaaaaaaaaaaaaa%22%7D%2C%22_id%22%3A%22exampleaaaaaaaaaaaaaaaaaaaaaaaaa4%22%2C%22ip%22%3A%22127.0.0.1%22%2C%22location%22%3A%7B%22country_short%22%3A%22US%22%2C%22country%22%3A%22United+States%22%2C%22region%22%3A%22Oklahoma%22%2C%22city%22%3A%22Oklahoma+City%22%2C%22latitude%22%3A35.4675598145%2C%22longitude%22%3A-97.5164337158%2C%22postal_code%22%3A%2273101%22%2C%22timezone%22%3A%22-05%3A00%22%7D%2C%22user_agent%22%3A%22Mozilla%5C%2F5.0+%28Macintosh%3B+U%3B+Intel+Mac+OS+X+10.6%3B+en-US%3B+rv%3A1.9.1.8%29+Gecko%5C%2F20100317+Postbox%5C%2F1.1.3%22%2C%22user_agent_parsed%22%3A%7B%22type%22%3A%22Email+Client%22%2C%22ua_family%22%3A%22Postbox%22%2C%22ua_name%22%3A%22Postbox+1.1.3%22%2C%22ua_version%22%3A%221.1.3%22%2C%22ua_url%22%3A%22http%3A%5C%2F%5C%2Fwww.postbox-inc.com%5C%2F%22%2C%22ua_company%22%3A%22Postbox%2C+Inc.%22%2C%22ua_company_url%22%3A%22http%3A%5C%2F%5C%2Fwww.postbox-inc.com%5C%2F%22%2C%22ua_icon%22%3A%22http%3A%5C%2F%5C%2Fcdn.mandrill.com%5C%2Fimg%5C%2Femail-client-icons%5C%2Fpostbox.png%22%2C%22os_family%22%3A%22OS+X%22%2C%22os_name%22%3A%22OS+X+10.6+Snow+Leopard%22%2C%22os_url%22%3A%22http%3A%5C%2F%5C%2Fwww.apple.com%5C%2Fosx%5C%2F%22%2C%22os_company%22%3A%22Apple+Computer%2C+Inc.%22%2C%22os_company_url%22%3A%22http%3A%5C%2F%5C%2Fwww.apple.com%5C%2F%22%2C%22os_icon%22%3A%22http%3A%5C%2F%5C%2Fcdn.mandrill.com%5C%2Fimg%5C%2Femail-client-icons%5C%2Fmacosx.png%22%2C%22mobile%22%3Afalse%7D%2C%22ts%22%3A1415267366%7D%2C%7B%22event%22%3A%22click%22%2C%22msg%22%3A%7B%22ts%22%3A1365109999%2C%22subject%22%3A%22This+an+example+webhook+message%22%2C%22email%22%3A%22example.webhook%40mandrillapp.com%22%2C%22sender%22%3A%22example.sender%40mandrillapp.com%22%2C%22tags%22%3A%5B%22webhook-example%22%5D%2C%22opens%22%3A%5B%7B%22ts%22%3A1365111111%7D%5D%2C%22clicks%22%3A%5B%7B%22ts%22%3A1365111111%2C%22url%22%3A%22http%3A%5C%2F%5C%2Fmandrill.com%22%7D%5D%2C%22state%22%3A%22sent%22%2C%22metadata%22%3A%7B%22user_id%22%3A111%7D%2C%22_id%22%3A%22exampleaaaaaaaaaaaaaaaaaaaaaaaaa5%22%2C%22_version%22%3A%22exampleaaaaaaaaaaaaaaa%22%7D%2C%22_id%22%3A%22exampleaaaaaaaaaaaaaaaaaaaaaaaaa5%22%2C%22ip%22%3A%22127.0.0.1%22%2C%22location%22%3A%7B%22country_short%22%3A%22US%22%2C%22country%22%3A%22United+States%22%2C%22region%22%3A%22Oklahoma%22%2C%22city%22%3A%22Oklahoma+City%22%2C%22latitude%22%3A35.4675598145%2C%22longitude%22%3A-97.5164337158%2C%22postal_code%22%3A%2273101%22%2C%22timezone%22%3A%22-05%3A00%22%7D%2C%22user_agent%22%3A%22Mozilla%5C%2F5.0+%28Macintosh%3B+U%3B+Intel+Mac+OS+X+10.6%3B+en-US%3B+rv%3A1.9.1.8%29+Gecko%5C%2F20100317+Postbox%5C%2F1.1.3%22%2C%22user_agent_parsed%22%3A%7B%22type%22%3A%22Email+Client%22%2C%22ua_family%22%3A%22Postbox%22%2C%22ua_name%22%3A%22Postbox+1.1.3%22%2C%22ua_version%22%3A%221.1.3%22%2C%22ua_url%22%3A%22http%3A%5C%2F%5C%2Fwww.postbox-inc.com%5C%2F%22%2C%22ua_company%22%3A%22Postbox%2C+Inc.%22%2C%22ua_company_url%22%3A%22http%3A%5C%2F%5C%2Fwww.postbox-inc.com%5C%2F%22%2C%22ua_icon%22%3A%22http%3A%5C%2F%5C%2Fcdn.mandrill.com%5C%2Fimg%5C%2Femail-client-icons%5C%2Fpostbox.png%22%2C%22os_family%22%3A%22OS+X%22%2C%22os_name%22%3A%22OS+X+10.6+Snow+Leopard%22%2C%22os_url%22%3A%22http%3A%5C%2F%5C%2Fwww.apple.com%5C%2Fosx%5C%2F%22%2C%22os_company%22%3A%22Apple+Computer%2C+Inc.%22%2C%22os_company_url%22%3A%22http%3A%5C%2F%5C%2Fwww.apple.com%5C%2F%22%2C%22os_icon%22%3A%22http%3A%5C%2F%5C%2Fcdn.mandrill.com%5C%2Fimg%5C%2Femail-client-icons%5C%2Fmacosx.png%22%2C%22mobile%22%3Afalse%7D%2C%22url%22%3A%22http%3A%5C%2F%5C%2Fmandrill.com%22%2C%22ts%22%3A1415267366%7D%2C%7B%22event%22%3A%22spam%22%2C%22msg%22%3A%7B%22ts%22%3A1365109999%2C%22subject%22%3A%22This+an+example+webhook+message%22%2C%22email%22%3A%22example.webhook%40mandrillapp.com%22%2C%22sender%22%3A%22example.sender%40mandrillapp.com%22%2C%22tags%22%3A%5B%22webhook-example%22%5D%2C%22opens%22%3A%5B%7B%22ts%22%3A1365111111%7D%5D%2C%22clicks%22%3A%5B%7B%22ts%22%3A1365111111%2C%22url%22%3A%22http%3A%5C%2F%5C%2Fmandrill.com%22%7D%5D%2C%22state%22%3A%22sent%22%2C%22metadata%22%3A%7B%22user_id%22%3A111%7D%2C%22_id%22%3A%22exampleaaaaaaaaaaaaaaaaaaaaaaaaa6%22%2C%22_version%22%3A%22exampleaaaaaaaaaaaaaaa%22%7D%2C%22_id%22%3A%22exampleaaaaaaaaaaaaaaaaaaaaaaaaa6%22%2C%22ts%22%3A1415267366%7D%2C%7B%22event%22%3A%22unsub%22%2C%22msg%22%3A%7B%22ts%22%3A1365109999%2C%22subject%22%3A%22This+an+example+webhook+message%22%2C%22email%22%3A%22example.webhook%40mandrillapp.com%22%2C%22sender%22%3A%22example.sender%40mandrillapp.com%22%2C%22tags%22%3A%5B%22webhook-example%22%5D%2C%22opens%22%3A%5B%7B%22ts%22%3A1365111111%7D%5D%2C%22clicks%22%3A%5B%7B%22ts%22%3A1365111111%2C%22url%22%3A%22http%3A%5C%2F%5C%2Fmandrill.com%22%7D%5D%2C%22state%22%3A%22sent%22%2C%22metadata%22%3A%7B%22user_id%22%3A111%7D%2C%22_id%22%3A%22exampleaaaaaaaaaaaaaaaaaaaaaaaaa7%22%2C%22_version%22%3A%22exampleaaaaaaaaaaaaaaa%22%7D%2C%22_id%22%3A%22exampleaaaaaaaaaaaaaaaaaaaaaaaaa7%22%2C%22ts%22%3A1415267366%7D%2C%7B%22event%22%3A%22reject%22%2C%22msg%22%3A%7B%22ts%22%3A1365109999%2C%22subject%22%3A%22This+an+example+webhook+message%22%2C%22email%22%3A%22example.webhook%40mandrillapp.com%22%2C%22sender%22%3A%22example.sender%40mandrillapp.com%22%2C%22tags%22%3A%5B%22webhook-example%22%5D%2C%22opens%22%3A%5B%5D%2C%22clicks%22%3A%5B%5D%2C%22state%22%3A%22rejected%22%2C%22metadata%22%3A%7B%22user_id%22%3A111%7D%2C%22_id%22%3A%22exampleaaaaaaaaaaaaaaaaaaaaaaaaa8%22%2C%22_version%22%3A%22exampleaaaaaaaaaaaaaaa%22%7D%2C%22_id%22%3A%22exampleaaaaaaaaaaaaaaaaaaaaaaaaa8%22%2C%22ts%22%3A1415267366%7D%5D"
    val payload = CollectorPayload(Shared.api, Nil, ContentType.some, body.some, Shared.cljSource, Shared.context)
    val actual = MandrillAdapter.toRawEvents(payload)
    actual must beFailing(NonEmptyList("Request body is formatted incorrectly: cannot process"))
  }
}
