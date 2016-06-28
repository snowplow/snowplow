/*
 * Copyright (c) 2016 Snowplow Analytics Ltd. All rights reserved.
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

class MailgunAdapterSpec extends Specification with DataTables with ValidationMatchers with ScalaCheck { def is =

  "This is a specification to test the MailgunAdapter functionality"                                            ^
                                                                                                                   p^
  "toRawEvents must return a Success Nel if every event 'delivered' in the payload is successful"                 ! e1^
  "toRawEvents must return a Success Nel if every event 'opened' in the payload is successful"                    ! e2^
  "toRawEvents must return a Success Nel if every event 'clicked' in the payload is successful"                   ! e3^
  "toRawEvents must return a Success Nel if every event 'unsubscribed' in the payload is successful"              ! e4^
  "toRawEvents must return a Nel Failure if the request body is missing"                                          ! e5^
  "toRawEvents must return a Nel Failure if the content type is missing"                                          ! e6^
  "toRawEvents must return a Nel Failure if the content type is incorrect"                                        ! e7^
  "toRawEvents must return a Failure Nel if the request body is empty"                                            ! e8^
  "toRawEvents must return a Failure if the request body could not be parsed"                                     ! e9^                                                                                                                
  "toRawEvents must return a Failure if the request body does not contain an event parameter"                     ! e10^
  "toRawEvents must return a Failure if the event type is not recognized"                                         ! e11^
  "payloadBodyToEvent must return a Failure if the event data is missing 'timestamp'"                             ! e12^
  "payloadBodyToEvent must return a Failure if the event data is missing 'token'"                                 ! e13^
  "payloadBodyToEvent must return a Failure if the event data is missing 'signature'"                             ! e14^
                                                                                                                  end
                                                                                                                  

  implicit val resolver = SpecHelpers.IgluResolver

  object Shared {
    val api = CollectorApi("com.mailgun", "v1")
    val cljSource = CollectorSource("clj-tomcat", "UTF-8", None)
    val context = CollectorContext(DateTime.parse("2013-08-29T00:18:48.000+00:00").some, "37.157.33.123".some, None, None, Nil, None)
  }                                            
  
  val ContentType = "application/x-www-form-urlencoded"

  def e1 = {
    val body = "X-Mailgun-Sid=WyIxZjQzMiIsICJyb25ueUBrZGUub3JnIiwgIjliMjYwIl0%3D&domain=sandboxbcd3ccb1a529415db665622619a61616.mailgun.org&message-headers=%5B%5B%22Sender%22%2C+%22postmaster%40sandboxbcd3ccb1a529415db665622619a61616.mailgun.org%22%5D%2C+%5B%22Date%22%2C+%22Mon%2C+27+Jun+2016+15%3A19%3A02+%2B0000%22%5D%2C+%5B%22X-Mailgun-Sid%22%2C+%22WyIxZjQzMiIsICJyb25ueUBrZGUub3JnIiwgIjliMjYwIl0%3D%22%5D%2C+%5B%22Received%22%2C+%22by+luna.mailgun.net+with+HTTP%3B+Mon%2C+27+Jun+2016+15%3A19%3A01+%2B0000%22%5D%2C+%5B%22Message-Id%22%2C+%22%3C20160627151901.3295.78981.1336C636%40sandboxbcd3ccb1a529415db665622619a61616.mailgun.org%3E%22%5D%2C+%5B%22To%22%2C+%22Ronny+%3Ctest%40snowplowanalytics.com%3E%22%5D%2C+%5B%22From%22%2C+%22Mailgun+Sandbox+%3Cpostmaster%40sandboxbcd3ccb1a529415db665622619a61616.mailgun.org%3E%22%5D%2C+%5B%22Subject%22%2C+%22Hello+Ronny%22%5D%2C+%5B%22Content-Type%22%2C+%5B%22text%2Fplain%22%2C+%7B%22charset%22%3A+%22ascii%22%7D%5D%5D%2C+%5B%22Mime-Version%22%2C+%221.0%22%5D%2C+%5B%22Content-Transfer-Encoding%22%2C+%5B%227bit%22%2C+%7B%7D%5D%5D%5D&Message-Id=%3C20160627151901.3295.78981.1336C636%40sandboxbcd3ccb1a529415db665622619a61616.mailgun.org%3E&recipient=test%40snowplowanalytics.com&event=delivered&timestamp=1467040750&token=c2fc6a36198fa651243afb6042867b7490e480843198008c6b&signature=9387fb0e5ff02de5e159594173f02c95c55d7e681b40a7b930ed4d0a3cbbdd6e&body-plain="    
    val payload = CollectorPayload(Shared.api, Nil, ContentType.some, body.some, Shared.cljSource, Shared.context)
    val expectedJson = 
      """|{
          |"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0",
          |"data":{
            |"schema":"iglu:com.mailgun/message_delivered/jsonschema/1-0-0",
            |"data":{
              |"recipient":"test@snowplowanalytics.com",
              |"message-headers":"[[\"Sender\", \"postmaster@sandboxbcd3ccb1a529415db665622619a61616.mailgun.org\"], [\"Date\", \"Mon, 27 Jun 2016 15:19:02 +0000\"], [\"X-Mailgun-Sid\", \"WyIxZjQzMiIsICJyb25ueUBrZGUub3JnIiwgIjliMjYwIl0=\"], [\"Received\", \"by luna.mailgun.net with HTTP; Mon, 27 Jun 2016 15:19:01 +0000\"], [\"Message-Id\", \"<20160627151901.3295.78981.1336C636@sandboxbcd3ccb1a529415db665622619a61616.mailgun.org>\"], [\"To\", \"Ronny <test@snowplowanalytics.com>\"], [\"From\", \"Mailgun Sandbox <postmaster@sandboxbcd3ccb1a529415db665622619a61616.mailgun.org>\"], [\"Subject\", \"Hello Ronny\"], [\"Content-Type\", [\"text/plain\", {\"charset\": \"ascii\"}]], [\"Mime-Version\", \"1.0\"], [\"Content-Transfer-Encoding\", [\"7bit\", {}]]]",
              |"body-plain":"",
              |"timestamp":"1467040750",
              |"event":"delivered",
              |"domain":"sandboxbcd3ccb1a529415db665622619a61616.mailgun.org",
              |"signature":"9387fb0e5ff02de5e159594173f02c95c55d7e681b40a7b930ed4d0a3cbbdd6e",
              |"token":"c2fc6a36198fa651243afb6042867b7490e480843198008c6b",
              |"Message-Id":"<20160627151901.3295.78981.1336C636@sandboxbcd3ccb1a529415db665622619a61616.mailgun.org>",
              |"X-Mailgun-Sid":"WyIxZjQzMiIsICJyb25ueUBrZGUub3JnIiwgIjliMjYwIl0="
            |}
          |}
        |}""".stripMargin.replaceAll("[\n\r]","")
    
    val expected = NonEmptyList(RawEvent(Shared.api,Map("tv" -> "com.mailgun-v1", "e" -> "ue", "p" -> "srv", "ue_pr" -> expectedJson), ContentType.some, Shared.cljSource, Shared.context))
    MailgunAdapter.toRawEvents(payload) must beSuccessful(expected)
  }

  def e2 = {
    val body = "city=San+Francisco&domain=sandboxbcd3ccb1a529415db665622619a61616.mailgun.org&device-type=desktop&my_var_1=Mailgun+Variable+%231&country=US&region=CA&client-name=Chrome&user-agent=Mozilla%2F5.0+%28X11%3B+Linux+x86_64%29+AppleWebKit%2F537.31+%28KHTML%2C+like+Gecko%29+Chrome%2F26.0.1410.43+Safari%2F537.31&client-os=Linux&my-var-2=awesome&ip=50.56.129.169&client-type=browser&recipient=alice%40example.com&event=opened&timestamp=1467297128&token=c2eecf923f9820812338de117346d6448ea2cf7e2e98cfa1a0&signature=9c70b687ef784ec5ed78f4d9442d641a9cfc7b909f9bf43d9ce7e44b3448cf97&body-plain="
    val payload = CollectorPayload(Shared.api, Nil, ContentType.some, body.some, Shared.cljSource, Shared.context)
    val expectedJson = 
      """|{
          |"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0",
          |"data":{
            |"schema":"iglu:com.mailgun/message_opened/jsonschema/1-0-0",
            |"data":{
              |"recipient":"alice@example.com",
              |"city":"San Francisco",
              |"ip":"50.56.129.169",
              |"body-plain":"",
              |"timestamp":"1467297128",
              |"event":"opened",
              |"user-agent":"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.31 (KHTML, like Gecko) Chrome/26.0.1410.43 Safari/537.31",
              |"domain":"sandboxbcd3ccb1a529415db665622619a61616.mailgun.org",
              |"signature":"9c70b687ef784ec5ed78f4d9442d641a9cfc7b909f9bf43d9ce7e44b3448cf97",
              |"country":"US",
              |"client-type":"browser",
              |"client-os":"Linux",
              |"token":"c2eecf923f9820812338de117346d6448ea2cf7e2e98cfa1a0",
              |"client-name":"Chrome",
              |"region":"CA",
              |"device-type":"desktop",
              |"my_var_1":"Mailgun Variable #1",
              |"my-var-2":"awesome"
            |}
          |}
        |}""".stripMargin.replaceAll("[\n\r]","")
    
    val expected = NonEmptyList(RawEvent(Shared.api,Map("tv" -> "com.mailgun-v1", "e" -> "ue", "p" -> "srv", "ue_pr" -> expectedJson), ContentType.some, Shared.cljSource, Shared.context))
    MailgunAdapter.toRawEvents(payload) must beSuccessful(expected)
  }

  def e3 = {
    val body = "city=San+Francisco&domain=sandboxbcd3ccb1a529415db665622619a61616.mailgun.org&device-type=desktop&my_var_1=Mailgun+Variable+%231&country=US&region=CA&client-name=Chrome&user-agent=Mozilla%2F5.0+%28X11%3B+Linux+x86_64%29+AppleWebKit%2F537.31+%28KHTML%2C+like+Gecko%29+Chrome%2F26.0.1410.43+Safari%2F537.31&client-os=Linux&my-var-2=awesome&url=http%3A%2F%2Fmailgun.net&ip=50.56.129.169&client-type=browser&recipient=alice%40example.com&event=clicked&timestamp=1467297069&token=cd89cd860be0e318371f4220b7e0f368b60ac9ab066354737f&signature=ffe2d315a1d937bd09d9f5c35ddac1eb448818e2203f5a41e3a7bd1fb47da385&body-plain="
    val payload = CollectorPayload(Shared.api, Nil, ContentType.some, body.some, Shared.cljSource, Shared.context)
    val expectedJson = 
      """|{
          |"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0",
          |"data":{
            |"schema":"iglu:com.mailgun/message_clicked/jsonschema/1-0-0",
            |"data":{
              |"recipient":"alice@example.com",
              |"city":"San Francisco",
              |"ip":"50.56.129.169",
              |"body-plain":"",
              |"timestamp":"1467297069",
              |"url":"http://mailgun.net",
              |"event":"clicked",
              |"user-agent":"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.31 (KHTML, like Gecko) Chrome/26.0.1410.43 Safari/537.31",
              |"domain":"sandboxbcd3ccb1a529415db665622619a61616.mailgun.org",
              |"signature":"ffe2d315a1d937bd09d9f5c35ddac1eb448818e2203f5a41e3a7bd1fb47da385",
              |"country":"US",
              |"client-type":"browser",
              |"client-os":"Linux",
              |"token":"cd89cd860be0e318371f4220b7e0f368b60ac9ab066354737f",
              |"client-name":"Chrome",
              |"region":"CA",
              |"device-type":"desktop",
              |"my_var_1":"Mailgun Variable #1",
              |"my-var-2":"awesome"
            |}
          |}
        |}""".stripMargin.replaceAll("[\n\r]","")
    
    val expected = NonEmptyList(RawEvent(Shared.api,Map("tv" -> "com.mailgun-v1", "e" -> "ue", "p" -> "srv", "ue_pr" -> expectedJson), ContentType.some, Shared.cljSource, Shared.context))
    MailgunAdapter.toRawEvents(payload) must beSuccessful(expected)
  }

  def e4 = {
    val body = "ip=50.56.129.169&city=San+Francisco&domain=sandboxbcd3ccb1a529415db665622619a61616.mailgun.org&device-type=desktop&my_var_1=Mailgun+Variable+%231&country=US&region=CA&client-name=Chrome&user-agent=Mozilla%2F5.0+%28X11%3B+Linux+x86_64%29+AppleWebKit%2F537.31+%28KHTML%2C+like+Gecko%29+Chrome%2F26.0.1410.43+Safari%2F537.31&client-os=Linux&my-var-2=awesome&client-type=browser&tag=%2A&recipient=alice%40example.com&event=unsubscribed&timestamp=1467297059&token=45272007729d82a7f7471d17e21298ee1a3899df65ba4a63ff&signature=150f32facb18c47273cf890d4aa13354ea789ad7b076554e8b324be6f446e2ad&body-plain="
    val payload = CollectorPayload(Shared.api, Nil, ContentType.some, body.some, Shared.cljSource, Shared.context)
    val expectedJson = 
      """|{
          |"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0",
          |"data":{
            |"schema":"iglu:com.mailgun/recipient_unsubscribed/jsonschema/1-0-0",
            |"data":{
              |"recipient":"alice@example.com",
              |"city":"San Francisco",
              |"ip":"50.56.129.169",
              |"body-plain":"",
              |"timestamp":"1467297059",
              |"event":"unsubscribed",
              |"tag":"*",
              |"user-agent":"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.31 (KHTML, like Gecko) Chrome/26.0.1410.43 Safari/537.31",
              |"domain":"sandboxbcd3ccb1a529415db665622619a61616.mailgun.org",
              |"signature":"150f32facb18c47273cf890d4aa13354ea789ad7b076554e8b324be6f446e2ad",
              |"country":"US",
              |"client-type":"browser",
              |"client-os":"Linux",
              |"token":"45272007729d82a7f7471d17e21298ee1a3899df65ba4a63ff",
              |"client-name":"Chrome",
              |"region":"CA",
              |"device-type":"desktop",
              |"my_var_1":"Mailgun Variable #1",
              |"my-var-2":"awesome"
            |}
          |}
        |}""".stripMargin.replaceAll("[\n\r]","")
    
    val expected = NonEmptyList(RawEvent(Shared.api,Map("tv" -> "com.mailgun-v1", "e" -> "ue", "p" -> "srv", "ue_pr" -> expectedJson), ContentType.some, Shared.cljSource, Shared.context))
    MailgunAdapter.toRawEvents(payload) must beSuccessful(expected)
  }

  def e5 = {
    val payload = CollectorPayload(Shared.api, Nil, ContentType.some, None, Shared.cljSource, Shared.context)
    MailgunAdapter.toRawEvents(payload) must beFailing(NonEmptyList("Request body is empty: no Mailgun events to process"))
  }

  def e6 = {
    val body = ""    
    val payload = CollectorPayload(Shared.api, Nil, None, body.some, Shared.cljSource, Shared.context)
    MailgunAdapter.toRawEvents(payload) must beFailing(NonEmptyList("Request body provided but content type empty, expected application/x-www-form-urlencoded for Mailgun"))
  }

  def e7 = {
    val body = ""    
    val ct = "application/json"
    val payload = CollectorPayload(Shared.api, Nil, ct.some, body.some, Shared.cljSource, Shared.context)
    MailgunAdapter.toRawEvents(payload) must beFailing(NonEmptyList("Content type of application/json provided, expected application/x-www-form-urlencoded for Mailgun"))
  }

  def e8 = {
    val body = ""
    val payload = CollectorPayload(Shared.api, Nil, ContentType.some, body.some, Shared.cljSource, Shared.context)
    val expected = NonEmptyList("Mailgun event body is empty: nothing to process")
    MailgunAdapter.toRawEvents(payload) must beFailing(expected)
  }

  def e9 = {
    val body = "X-MailgunSid=WyIxZjQzMiIsICJyb25ueUBrZGUub3JnIiwgIjliMjYwIl0%3D&event=delivered&timestamp=1467040750&token=c2fc6a36198fa651243afb6042867b7490e480843198008c6b&signature=9387fb0e5ff02de5e159594173f02c95c55d7e681b40a7b930ed4d0a3cbbdd6e&recipient=<>"    
    val payload = CollectorPayload(Shared.api, Nil, ContentType.some, body.some, Shared.cljSource, Shared.context)
    val expected = NonEmptyList("Mailgun could not parse body: [java.lang.IllegalArgumentException: Illegal character in query at index 261: http://localhost/?X-MailgunSid=WyIxZjQzMiIsICJyb25ueUBrZGUub3JnIiwgIjliMjYwIl0%3D&event=delivered&timestamp=1467040750&token=c2fc6a36198fa651243afb6042867b7490e480843198008c6b&signature=9387fb0e5ff02de5e159594173f02c95c55d7e681b40a7b930ed4d0a3cbbdd6e&recipient=<>]")
    MailgunAdapter.toRawEvents(payload) must beFailing(expected)
  }

  def e10 = {
    val body = "X-MailgunSid=WyIxZjQzMiIsICJyb25ueUBrZGUub3JnIiwgIjliMjYwIl0%3D&timestamp=1467040750&token=c2fc6a36198fa651243afb6042867b7490e480843198008c6b&signature=9387fb0e5ff02de5e159594173f02c95c55d7e681b40a7b930ed4d0a3cbbdd6e"    
    val payload = CollectorPayload(Shared.api, Nil, ContentType.some, body.some, Shared.cljSource, Shared.context)
    val expected = NonEmptyList("No Mailgun event parameter provided: cannot determine event type")
    MailgunAdapter.toRawEvents(payload) must beFailing(expected)
  }

  def e11 = {
    val body = "X-MailgunSid=WyIxZjQzMiIsICJyb25ueUBrZGUub3JnIiwgIjliMjYwIl0%3D&event=released&timestamp=1467040750&token=c2fc6a36198fa651243afb6042867b7490e480843198008c6b&signature=9387fb0e5ff02de5e159594173f02c95c55d7e681b40a7b930ed4d0a3cbbdd6e"    
    val payload = CollectorPayload(Shared.api, Nil, ContentType.some, body.some, Shared.cljSource, Shared.context)
    val expected = NonEmptyList("Mailgun event failed: type parameter [released] not recognized")
    MailgunAdapter.toRawEvents(payload) must beFailing(expected)
  }

  def e12 = {
    val body = "X-Mailgun-Sid=WyIxZjQzMiIsICJyb25ueUBrZGUub3JnIiwgIjliMjYwIl0%3D&domain=sandboxbcd3ccb1a529415db665622619a61616.mailgun.org&message-headers=%5B%5B%22Sender%22%2C+%22postmaster%40sandboxbcd3ccb1a529415db665622619a61616.mailgun.org%22%5D%2C+%5B%22Date%22%2C+%22Mon%2C+27+Jun+2016+15%3A19%3A02+%2B0000%22%5D%2C+%5B%22X-Mailgun-Sid%22%2C+%22WyIxZjQzMiIsICJyb25ueUBrZGUub3JnIiwgIjliMjYwIl0%3D%22%5D%2C+%5B%22Received%22%2C+%22by+luna.mailgun.net+with+HTTP%3B+Mon%2C+27+Jun+2016+15%3A19%3A01+%2B0000%22%5D%2C+%5B%22Message-Id%22%2C+%22%3C20160627151901.3295.78981.1336C636%40sandboxbcd3ccb1a529415db665622619a61616.mailgun.org%3E%22%5D%2C+%5B%22To%22%2C+%22Ronny+%3Ctest%40snowplowanalytics.com%3E%22%5D%2C+%5B%22From%22%2C+%22Mailgun+Sandbox+%3Cpostmaster%40sandboxbcd3ccb1a529415db665622619a61616.mailgun.org%3E%22%5D%2C+%5B%22Subject%22%2C+%22Hello+Ronny%22%5D%2C+%5B%22Content-Type%22%2C+%5B%22text%2Fplain%22%2C+%7B%22charset%22%3A+%22ascii%22%7D%5D%5D%2C+%5B%22Mime-Version%22%2C+%221.0%22%5D%2C+%5B%22Content-Transfer-Encoding%22%2C+%5B%227bit%22%2C+%7B%7D%5D%5D%5D&Message-Id=%3C20160627151901.3295.78981.1336C636%40sandboxbcd3ccb1a529415db665622619a61616.mailgun.org%3E&recipient=test%40snowplowanalytics.com&event=delivered&token=c2fc6a36198fa651243afb6042867b7490e480843198008c6b&signature=9387fb0e5ff02de5e159594173f02c95c55d7e681b40a7b930ed4d0a3cbbdd6e&body-plain="    
    val payload = CollectorPayload(Shared.api, Nil, ContentType.some, body.some, Shared.cljSource, Shared.context)
    val expected = NonEmptyList("Mailgun event data missing 'timestamp'")
    MailgunAdapter.toRawEvents(payload) must beFailing(expected)
  }

  def e13 = {
    val body = "X-Mailgun-Sid=WyIxZjQzMiIsICJyb25ueUBrZGUub3JnIiwgIjliMjYwIl0%3D&domain=sandboxbcd3ccb1a529415db665622619a61616.mailgun.org&message-headers=%5B%5B%22Sender%22%2C+%22postmaster%40sandboxbcd3ccb1a529415db665622619a61616.mailgun.org%22%5D%2C+%5B%22Date%22%2C+%22Mon%2C+27+Jun+2016+15%3A19%3A02+%2B0000%22%5D%2C+%5B%22X-Mailgun-Sid%22%2C+%22WyIxZjQzMiIsICJyb25ueUBrZGUub3JnIiwgIjliMjYwIl0%3D%22%5D%2C+%5B%22Received%22%2C+%22by+luna.mailgun.net+with+HTTP%3B+Mon%2C+27+Jun+2016+15%3A19%3A01+%2B0000%22%5D%2C+%5B%22Message-Id%22%2C+%22%3C20160627151901.3295.78981.1336C636%40sandboxbcd3ccb1a529415db665622619a61616.mailgun.org%3E%22%5D%2C+%5B%22To%22%2C+%22Ronny+%3Ctest%40snowplowanalytics.com%3E%22%5D%2C+%5B%22From%22%2C+%22Mailgun+Sandbox+%3Cpostmaster%40sandboxbcd3ccb1a529415db665622619a61616.mailgun.org%3E%22%5D%2C+%5B%22Subject%22%2C+%22Hello+Ronny%22%5D%2C+%5B%22Content-Type%22%2C+%5B%22text%2Fplain%22%2C+%7B%22charset%22%3A+%22ascii%22%7D%5D%5D%2C+%5B%22Mime-Version%22%2C+%221.0%22%5D%2C+%5B%22Content-Transfer-Encoding%22%2C+%5B%227bit%22%2C+%7B%7D%5D%5D%5D&Message-Id=%3C20160627151901.3295.78981.1336C636%40sandboxbcd3ccb1a529415db665622619a61616.mailgun.org%3E&recipient=test%40snowplowanalytics.com&event=delivered&timestamp=1467040750&signature=9387fb0e5ff02de5e159594173f02c95c55d7e681b40a7b930ed4d0a3cbbdd6e&body-plain="    
    val payload = CollectorPayload(Shared.api, Nil, ContentType.some, body.some, Shared.cljSource, Shared.context)
    val expected = NonEmptyList("Mailgun event data missing 'token'")
    MailgunAdapter.toRawEvents(payload) must beFailing(expected)
  }

  def e14 = {
    val body = "X-Mailgun-Sid=WyIxZjQzMiIsICJyb25ueUBrZGUub3JnIiwgIjliMjYwIl0%3D&domain=sandboxbcd3ccb1a529415db665622619a61616.mailgun.org&message-headers=%5B%5B%22Sender%22%2C+%22postmaster%40sandboxbcd3ccb1a529415db665622619a61616.mailgun.org%22%5D%2C+%5B%22Date%22%2C+%22Mon%2C+27+Jun+2016+15%3A19%3A02+%2B0000%22%5D%2C+%5B%22X-Mailgun-Sid%22%2C+%22WyIxZjQzMiIsICJyb25ueUBrZGUub3JnIiwgIjliMjYwIl0%3D%22%5D%2C+%5B%22Received%22%2C+%22by+luna.mailgun.net+with+HTTP%3B+Mon%2C+27+Jun+2016+15%3A19%3A01+%2B0000%22%5D%2C+%5B%22Message-Id%22%2C+%22%3C20160627151901.3295.78981.1336C636%40sandboxbcd3ccb1a529415db665622619a61616.mailgun.org%3E%22%5D%2C+%5B%22To%22%2C+%22Ronny+%3Ctest%40snowplowanalytics.com%3E%22%5D%2C+%5B%22From%22%2C+%22Mailgun+Sandbox+%3Cpostmaster%40sandboxbcd3ccb1a529415db665622619a61616.mailgun.org%3E%22%5D%2C+%5B%22Subject%22%2C+%22Hello+Ronny%22%5D%2C+%5B%22Content-Type%22%2C+%5B%22text%2Fplain%22%2C+%7B%22charset%22%3A+%22ascii%22%7D%5D%5D%2C+%5B%22Mime-Version%22%2C+%221.0%22%5D%2C+%5B%22Content-Transfer-Encoding%22%2C+%5B%227bit%22%2C+%7B%7D%5D%5D%5D&Message-Id=%3C20160627151901.3295.78981.1336C636%40sandboxbcd3ccb1a529415db665622619a61616.mailgun.org%3E&recipient=test%40snowplowanalytics.com&event=delivered&timestamp=1467040750&token=c2fc6a36198fa651243afb6042867b7490e480843198008c6b&body-plain="    
    val payload = CollectorPayload(Shared.api, Nil, ContentType.some, body.some, Shared.cljSource, Shared.context)
    val expected = NonEmptyList("Mailgun event data missing 'signature'")
    MailgunAdapter.toRawEvents(payload) must beFailing(expected)
  }  

}
