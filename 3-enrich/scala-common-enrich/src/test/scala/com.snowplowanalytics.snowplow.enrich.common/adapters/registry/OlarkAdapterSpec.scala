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

class OlarkAdapterSpec extends Specification with DataTables with ValidationMatchers with ScalaCheck { def is =

  "This is a specification to test the OlarkAdapter functionality"                                            ^
                                                                                                                   p^
  "toRawEvents must return a Success Nel if the transcript event in the payload is successful"                    ! e1^
  "toRawEvents must return a Success Nel if the offline message event in the payload is successful"               ! e2^
  "toRawEvents must return a Nel Failure if the request body is missing"                                          ! e3^
  "toRawEvents must return a Nel Failure if the content type is missing"                                          ! e4^
  "toRawEvents must return a Nel Failure if the content type is incorrect"                                        ! e5^
  "toRawEvents must return a Failure Nel if the event in the payload is empty"                                    ! e6^
  "payloadBodyToEvent must return a Failure if the event data does not have 'data' as a key"                      ! e7^
  "payloadBodyToEvent must return a Failure if the event string failed to parse into JSON"                        ! e8^
                                                                                                                   end

  implicit val resolver = SpecHelpers.IgluResolver

  object Shared {
    val api = CollectorApi("com.olark", "v1")
    val cljSource = CollectorSource("clj-tomcat", "UTF-8", None)
    val context = CollectorContext(DateTime.parse("2013-08-29T00:18:48.000+00:00").some, "37.157.33.123".some, None, None, Nil, None)
  }

  val ContentType = "application/x-www-form-urlencoded"

  def e1 = {
    val body = "data=%7B%22kind%22%3A%20%22Conversation%22%2C%20%22tags%22%3A%20%5B%22olark%22%2C%20%22customer%22%5D%2C%20%22items%22%3A%20%5B%7B%22body%22%3A%20%22Hi%20there.%20Need%20any%20help%3F%22%2C%20%22timestamp%22%3A%20%221307116657.1%22%2C%20%22kind%22%3A%20%22MessageToVisitor%22%2C%20%22nickname%22%3A%20%22John%22%2C%20%22operatorId%22%3A%20%221234%22%7D%2C%20%7B%22body%22%3A%20%22Yes%2C%20please%20help%20me%20with%20billing.%22%2C%20%22timestamp%22%3A%20%221307116661.25%22%2C%20%22kind%22%3A%20%22MessageToOperator%22%2C%20%22nickname%22%3A%20%22Bob%22%7D%5D%2C%20%22operators%22%3A%20%7B%221234%22%3A%20%7B%22username%22%3A%20%22jdoe%22%2C%20%22emailAddress%22%3A%20%22john%40example.com%22%2C%20%22kind%22%3A%20%22Operator%22%2C%20%22nickname%22%3A%20%22John%22%2C%20%22id%22%3A%20%221234%22%7D%7D%2C%20%22groups%22%3A%20%5B%7B%22kind%22%3A%20%22Group%22%2C%20%22name%22%3A%20%22My%20Sales%20Group%22%2C%20%22id%22%3A%20%220123456789abcdef%22%7D%5D%2C%20%22visitor%22%3A%20%7B%22ip%22%3A%20%22123.4.56.78%22%2C%20%22city%22%3A%20%22Palo%20Alto%22%2C%20%22kind%22%3A%20%22Visitor%22%2C%20%22conversationBeginPage%22%3A%20%22http%3A%2F%2Fwww.example.com%2Fpath%22%2C%20%22countryCode%22%3A%20%22US%22%2C%20%22country%22%3A%20%22United%20State%22%2C%20%22region%22%3A%20%22CA%22%2C%20%22chat_feedback%22%3A%20%7B%22overall_chat%22%3A%205%2C%20%22responsiveness%22%3A%205%2C%20%22friendliness%22%3A%205%2C%20%22knowledge%22%3A%205%2C%20%22comments%22%3A%20%22Very%20helpful%2C%20thanks%22%7D%2C%20%22operatingSystem%22%3A%20%22Windows%22%2C%20%22emailAddress%22%3A%20%22bob%40example.com%22%2C%20%22organization%22%3A%20%22Widgets%20Inc.%22%2C%20%22phoneNumber%22%3A%20%22%28555%29%20555-5555%22%2C%20%22fullName%22%3A%20%22Bob%20Doe%22%2C%20%22customFields%22%3A%20%7B%22favoriteColor%22%3A%20%22blue%22%2C%20%22myInternalCustomerId%22%3A%20%2212341234%22%7D%2C%20%22id%22%3A%20%229QRF9YWM5XW3ZSU7P9CGWRU89944341%22%2C%20%22browser%22%3A%20%22Chrome%2012.1%22%7D%2C%20%22id%22%3A%20%22EV695BI2930A6XMO32886MPT899443414%22%7D"
    val payload = CollectorPayload(Shared.api, Nil, ContentType.some, body.some, Shared.cljSource, Shared.context)
    val expectedJson = 
      """|{
          |"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0",
          |"data":{
            |"schema":"iglu:com.olark/transcript/jsonschema/1-0-0",
            |"data":{
              |"kind":"Conversation",
              |"tags":["olark","customer"],
              |"items":[{
                |"body":"Hi there. Need any help?",
                |"timestamp":"1307116657.1",
                |"kind":"MessageToVisitor",
                |"nickname":"John",
                |"operatorId":"1234"
              |},{
                |"body":"Yes, please help me with billing.",
                |"timestamp":"1307116661.25",
                |"kind":"MessageToOperator",
                |"nickname":"Bob"
              |}],
              |"operators":{
                |"1234":{
                  |"username":"jdoe",
                  |"emailAddress":"john@example.com",
                  |"kind":"Operator",
                  |"nickname":"John",
                  |"id":"1234"
                |}
              |},
              |"groups":[{
                |"kind":"Group",
                |"name":"My Sales Group",
                |"id":"0123456789abcdef"
              |}],
              |"visitor":{
                |"ip":"123.4.56.78",
                |"city":"Palo Alto",
                |"kind":"Visitor",
                |"conversationBeginPage":"http://www.example.com/path",
                |"countryCode":"US",
                |"country":"United State",
                |"region":"CA",
                |"chat_feedback":{
                  |"overall_chat":5,
                  |"responsiveness":5,
                  |"friendliness":5,
                  |"knowledge":5,
                  |"comments":"Very helpful, thanks"
                |},
                |"operatingSystem":"Windows",
                |"emailAddress":"bob@example.com",
                |"organization":"Widgets Inc.",
                |"phoneNumber":"(555) 555-5555",
                |"fullName":"Bob Doe",
                |"customFields":{
                  |"favoriteColor":"blue",
                  |"myInternalCustomerId":"12341234"
                |},
                |"id":"9QRF9YWM5XW3ZSU7P9CGWRU89944341",
                |"browser":"Chrome 12.1"
              |},
              |"id":"EV695BI2930A6XMO32886MPT899443414"
            |}
          |}
        |}""".stripMargin.replaceAll("[\n\r]","")
    val expected = NonEmptyList(RawEvent(Shared.api,Map("tv" -> "com.olark-v1", "e" -> "ue", "p" -> "srv", "ue_pr" -> expectedJson), ContentType.some, Shared.cljSource, Shared.context))
    OlarkAdapter.toRawEvents(payload) must beSuccessful(expected)
  }

  def e2 = {
    val body = "data=%7B%22kind%22%3A%20%22Conversation%22%2C%20%22id%22%3A%20%22EV695BI2930A6XMO32886QPT899443414%22%2C%20%22items%22%3A%20%5B%7B%22kind%22%3A%20%22OfflineMessage%22%2C%20%22timestamp%22%3A%20%221307116667.1%22%2C%20%22body%22%3A%20%22Hi%20there.%22%7D%5D%2C%20%22visitor%22%3A%20%7B%22kind%22%3A%20%22Visitor%22%2C%20%22id%22%3A%20%229QRF9YWM5XW3ZSU7P9CGWRU89944341%22%2C%20%22fullName%22%3A%20%22John%20Doe%22%2C%20%22emailAddress%22%3A%20%22foo%40example.com%22%2C%20%22phoneNumber%22%3A%20%22%28555%29%20555-5555%22%2C%20%22city%22%3A%20%22Palo%20Alto%22%2C%20%22region%22%3A%20%22CA%22%2C%20%22country%22%3A%20%22United%20States%22%2C%20%22countryCode%22%3A%20%22US%22%2C%20%22organization%22%3A%20%22Widgets%20Inc.%22%2C%20%22ip%22%3A%20%22123.4.56.78%22%2C%20%22browser%22%3A%20%22Chrome%2012.1%22%2C%20%22operatingSystem%22%3A%20%22Windows%22%2C%20%22customFields%22%3A%20%7B%22myInternalCustomerId%22%3A%20%2212341234%22%2C%20%22favoriteColor%22%3A%20%22blue%22%7D%20%7D%2C%20%22groups%22%3A%20%5B%7B%22name%22%3A%20%22My%20Sales%20Group%22%2C%20%22id%22%3A%20%220123456789abcdef%22%2C%20%22kind%22%3A%20%22Group%22%7D%5D%20%7D"
    val payload = CollectorPayload(Shared.api, Nil, ContentType.some, body.some, Shared.cljSource, Shared.context)
    val expectedJson =
      """|{
          |"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0",
          |"data":{
            |"schema":"iglu:com.olark/offline_message/jsonschema/1-0-0",
            |"data":{
              |"kind":"Conversation",
              |"id":"EV695BI2930A6XMO32886QPT899443414",
              |"items":[{
                |"kind":"OfflineMessage",
                |"timestamp":"1307116667.1",
                |"body":"Hi there."
              |}],
              |"visitor":{
                |"kind":"Visitor",
                |"id":"9QRF9YWM5XW3ZSU7P9CGWRU89944341",
                |"fullName":"John Doe",
                |"emailAddress":"foo@example.com",
                |"phoneNumber":"(555) 555-5555",
                |"city":"Palo Alto",
                |"region":"CA",
                |"country":"United States",
                |"countryCode":"US",
                |"organization":"Widgets Inc.",
                |"ip":"123.4.56.78",
                |"browser":"Chrome 12.1",
                |"operatingSystem":"Windows",
                |"customFields":{
                  |"myInternalCustomerId":"12341234",
                  |"favoriteColor":"blue"
                |}
              |},
              |"groups":[{
                |"name":"My Sales Group",
                |"id":"0123456789abcdef",
                |"kind":"Group"
              |}]
            |}
          |}
        |}""".stripMargin.replaceAll("[\n\r]","")

    val expected = NonEmptyList(RawEvent(Shared.api,Map("tv" -> "com.olark-v1", "e" -> "ue", "p" -> "srv", "ue_pr" -> expectedJson), ContentType.some, Shared.cljSource, Shared.context))
    OlarkAdapter.toRawEvents(payload) must beSuccessful(expected)
  }

  def e3 = {
    val payload = CollectorPayload(Shared.api, Nil, ContentType.some, None, Shared.cljSource, Shared.context)
    OlarkAdapter.toRawEvents(payload) must beFailing(NonEmptyList("Request body is empty: no Olark events to process"))
  }

  def e4 = {
    val body = "data=%7B%22kind%22%3A%20%22Conversation%22%2C%20%22tags%22%3A%20%5B%22olark%22%2C%20%22customer%22%5D%2C%20%22items%22%3A%20%5B%7B%22body%22%3A%20%22Hi%20there.%20Need%20any%20help%3F%22%2C%20%22timestamp%22%3A%20%221307116657.1%22%2C%20%22kind%22%3A%20%22MessageToVisitor%22%2C%20%22nickname%22%3A%20%22John%22%2C%20%22operatorId%22%3A%20%221234%22%7D%2C%20%7B%22body%22%3A%20%22Yes%2C%20please%20help%20me%20with%20billing.%22%2C%20%22timestamp%22%3A%20%221307116661.25%22%2C%20%22kind%22%3A%20%22MessageToOperator%22%2C%20%22nickname%22%3A%20%22Bob%22%7D%5D%2C%20%22operators%22%3A%20%7B%221234%22%3A%20%7B%22username%22%3A%20%22jdoe%22%2C%20%22emailAddress%22%3A%20%22john%40example.com%22%2C%20%22kind%22%3A%20%22Operator%22%2C%20%22nickname%22%3A%20%22John%22%2C%20%22id%22%3A%20%221234%22%7D%7D%2C%20%22groups%22%3A%20%5B%7B%22kind%22%3A%20%22Group%22%2C%20%22name%22%3A%20%22My%20Sales%20Group%22%2C%20%22id%22%3A%20%220123456789abcdef%22%7D%5D%2C%20%22visitor%22%3A%20%7B%22ip%22%3A%20%22123.4.56.78%22%2C%20%22city%22%3A%20%22Palo%20Alto%22%2C%20%22kind%22%3A%20%22Visitor%22%2C%20%22conversationBeginPage%22%3A%20%22http%3A%2F%2Fwww.example.com%2Fpath%22%2C%20%22countryCode%22%3A%20%22US%22%2C%20%22country%22%3A%20%22United%20State%22%2C%20%22region%22%3A%20%22CA%22%2C%20%22chat_feedback%22%3A%20%7B%22overall_chat%22%3A%205%2C%20%22responsiveness%22%3A%205%2C%20%22friendliness%22%3A%205%2C%20%22knowledge%22%3A%205%2C%20%22comments%22%3A%20%22Very%20helpful%2C%20thanks%22%7D%2C%20%22operatingSystem%22%3A%20%22Windows%22%2C%20%22emailAddress%22%3A%20%22bob%40example.com%22%2C%20%22organization%22%3A%20%22Widgets%20Inc.%22%2C%20%22phoneNumber%22%3A%20%22%28555%29%20555-5555%22%2C%20%22fullName%22%3A%20%22Bob%20Doe%22%2C%20%22customFields%22%3A%20%7B%22favoriteColor%22%3A%20%22blue%22%2C%20%22myInternalCustomerId%22%3A%20%2212341234%22%7D%2C%20%22id%22%3A%20%229QRF9YWM5XW3ZSU7P9CGWRU89944341%22%2C%20%22browser%22%3A%20%22Chrome%2012.1%22%7D%2C%20%22id%22%3A%20%22EV695BI2930A6XMO32886MPT899443414%22%7D"
    val payload = CollectorPayload(Shared.api, Nil, None, body.some, Shared.cljSource, Shared.context)
    OlarkAdapter.toRawEvents(payload) must beFailing(NonEmptyList("Request body provided but content type empty, expected application/x-www-form-urlencoded for Olark"))
  }

  def e5 = {
    val body = "data=%7B%22kind%22%3A%20%22Conversation%22%2C%20%22tags%22%3A%20%5B%22olark%22%2C%20%22customer%22%5D%2C%20%22items%22%3A%20%5B%7B%22body%22%3A%20%22Hi%20there.%20Need%20any%20help%3F%22%2C%20%22timestamp%22%3A%20%221307116657.1%22%2C%20%22kind%22%3A%20%22MessageToVisitor%22%2C%20%22nickname%22%3A%20%22John%22%2C%20%22operatorId%22%3A%20%221234%22%7D%2C%20%7B%22body%22%3A%20%22Yes%2C%20please%20help%20me%20with%20billing.%22%2C%20%22timestamp%22%3A%20%221307116661.25%22%2C%20%22kind%22%3A%20%22MessageToOperator%22%2C%20%22nickname%22%3A%20%22Bob%22%7D%5D%2C%20%22operators%22%3A%20%7B%221234%22%3A%20%7B%22username%22%3A%20%22jdoe%22%2C%20%22emailAddress%22%3A%20%22john%40example.com%22%2C%20%22kind%22%3A%20%22Operator%22%2C%20%22nickname%22%3A%20%22John%22%2C%20%22id%22%3A%20%221234%22%7D%7D%2C%20%22groups%22%3A%20%5B%7B%22kind%22%3A%20%22Group%22%2C%20%22name%22%3A%20%22My%20Sales%20Group%22%2C%20%22id%22%3A%20%220123456789abcdef%22%7D%5D%2C%20%22visitor%22%3A%20%7B%22ip%22%3A%20%22123.4.56.78%22%2C%20%22city%22%3A%20%22Palo%20Alto%22%2C%20%22kind%22%3A%20%22Visitor%22%2C%20%22conversationBeginPage%22%3A%20%22http%3A%2F%2Fwww.example.com%2Fpath%22%2C%20%22countryCode%22%3A%20%22US%22%2C%20%22country%22%3A%20%22United%20State%22%2C%20%22region%22%3A%20%22CA%22%2C%20%22chat_feedback%22%3A%20%7B%22overall_chat%22%3A%205%2C%20%22responsiveness%22%3A%205%2C%20%22friendliness%22%3A%205%2C%20%22knowledge%22%3A%205%2C%20%22comments%22%3A%20%22Very%20helpful%2C%20thanks%22%7D%2C%20%22operatingSystem%22%3A%20%22Windows%22%2C%20%22emailAddress%22%3A%20%22bob%40example.com%22%2C%20%22organization%22%3A%20%22Widgets%20Inc.%22%2C%20%22phoneNumber%22%3A%20%22%28555%29%20555-5555%22%2C%20%22fullName%22%3A%20%22Bob%20Doe%22%2C%20%22customFields%22%3A%20%7B%22favoriteColor%22%3A%20%22blue%22%2C%20%22myInternalCustomerId%22%3A%20%2212341234%22%7D%2C%20%22id%22%3A%20%229QRF9YWM5XW3ZSU7P9CGWRU89944341%22%2C%20%22browser%22%3A%20%22Chrome%2012.1%22%7D%2C%20%22id%22%3A%20%22EV695BI2930A6XMO32886MPT899443414%22%7D"
    val ct = "application/json"
    val payload = CollectorPayload(Shared.api, Nil, ct.some, body.some, Shared.cljSource, Shared.context)
    OlarkAdapter.toRawEvents(payload) must beFailing(NonEmptyList("Content type of application/json provided, expected application/x-www-form-urlencoded for Olark"))
  }

  def e6 = {
    val body = ""
    val payload = CollectorPayload(Shared.api, Nil, ContentType.some, body.some, Shared.cljSource, Shared.context)
    val expected = NonEmptyList("Olark event body is empty: nothing to process")
    OlarkAdapter.toRawEvents(payload) must beFailing(expected)
  }

  def e7 = {
    val body = "%7B%22kind%22%3A%20%22Conversation%22%2C%20%22tags%22%3A%20%5B%22olark%22%2C%20%22customer%22%5D%2C%20%22items%22%3A%20%5B%7B%22body%22%3A%20%22Hi%20there.%20Need%20any%20help%3F%22%2C%20%22timestamp%22%3A%20%221307116657.1%22%2C%20%22kind%22%3A%20%22MessageToVisitor%22%2C%20%22nickname%22%3A%20%22John%22%2C%20%22operatorId%22%3A%20%221234%22%7D%2C%20%7B%22body%22%3A%20%22Yes%2C%20please%20help%20me%20with%20billing.%22%2C%20%22timestamp%22%3A%20%221307116661.25%22%2C%20%22kind%22%3A%20%22MessageToOperator%22%2C%20%22nickname%22%3A%20%22Bob%22%7D%5D%2C%20%22operators%22%3A%20%7B%221234%22%3A%20%7B%22username%22%3A%20%22jdoe%22%2C%20%22emailAddress%22%3A%20%22john%40example.com%22%2C%20%22kind%22%3A%20%22Operator%22%2C%20%22nickname%22%3A%20%22John%22%2C%20%22id%22%3A%20%221234%22%7D%7D%2C%20%22groups%22%3A%20%5B%7B%22kind%22%3A%20%22Group%22%2C%20%22name%22%3A%20%22My%20Sales%20Group%22%2C%20%22id%22%3A%20%220123456789abcdef%22%7D%5D%2C%20%22visitor%22%3A%20%7B%22ip%22%3A%20%22123.4.56.78%22%2C%20%22city%22%3A%20%22Palo%20Alto%22%2C%20%22kind%22%3A%20%22Visitor%22%2C%20%22conversationBeginPage%22%3A%20%22http%3A%2F%2Fwww.example.com%2Fpath%22%2C%20%22countryCode%22%3A%20%22US%22%2C%20%22country%22%3A%20%22United%20State%22%2C%20%22region%22%3A%20%22CA%22%2C%20%22chat_feedback%22%3A%20%7B%22overall_chat%22%3A%205%2C%20%22responsiveness%22%3A%205%2C%20%22friendliness%22%3A%205%2C%20%22knowledge%22%3A%205%2C%20%22comments%22%3A%20%22Very%20helpful%2C%20thanks%22%7D%2C%20%22operatingSystem%22%3A%20%22Windows%22%2C%20%22emailAddress%22%3A%20%22bob%40example.com%22%2C%20%22organization%22%3A%20%22Widgets%20Inc.%22%2C%20%22phoneNumber%22%3A%20%22%28555%29%20555-5555%22%2C%20%22fullName%22%3A%20%22Bob%20Doe%22%2C%20%22customFields%22%3A%20%7B%22favoriteColor%22%3A%20%22blue%22%2C%20%22myInternalCustomerId%22%3A%20%2212341234%22%7D%2C%20%22id%22%3A%20%229QRF9YWM5XW3ZSU7P9CGWRU89944341%22%2C%20%22browser%22%3A%20%22Chrome%2012.1%22%7D%2C%20%22id%22%3A%20%22EV695BI2930A6XMO32886MPT899443414%22%7D"
    val payload = CollectorPayload(Shared.api, Nil, ContentType.some, body.some, Shared.cljSource, Shared.context)
    val expected = NonEmptyList("Olark event data does not have 'data' as a key")
    OlarkAdapter.toRawEvents(payload) must beFailing(expected)
  }

  def e8 = {
    val body = "data=kind%22%3A%20%22Conversation%22%2C%20%22tags%22%3A%20%5B%22olark%22%2C%20%22customer%22%5D%2C%20%22items%22%3A%20%5B%7B%22body%22%3A%20%22Hi%20there.%20Need%20any%20help%3F%22%2C%20%22timestamp%22%3A%20%221307116657.1%22%2C%20%22kind%22%3A%20%22MessageToVisitor%22%2C%20%22nickname%22%3A%20%22John%22%2C%20%22operatorId%22%3A%20%221234%22%7D%2C%20%7B%22body%22%3A%20%22Yes%2C%20please%20help%20me%20with%20billing.%22%2C%20%22timestamp%22%3A%20%221307116661.25%22%2C%20%22kind%22%3A%20%22MessageToOperator%22%2C%20%22nickname%22%3A%20%22Bob%22%7D%5D%2C%20%22operators%22%3A%20%7B%221234%22%3A%20%7B%22username%22%3A%20%22jdoe%22%2C%20%22emailAddress%22%3A%20%22john%40example.com%22%2C%20%22kind%22%3A%20%22Operator%22%2C%20%22nickname%22%3A%20%22John%22%2C%20%22id%22%3A%20%221234%22%7D%7D%2C%20%22groups%22%3A%20%5B%7B%22kind%22%3A%20%22Group%22%2C%20%22name%22%3A%20%22My%20Sales%20Group%22%2C%20%22id%22%3A%20%220123456789abcdef%22%7D%5D%2C%20%22visitor%22%3A%20%7B%22ip%22%3A%20%22123.4.56.78%22%2C%20%22city%22%3A%20%22Palo%20Alto%22%2C%20%22kind%22%3A%20%22Visitor%22%2C%20%22conversationBeginPage%22%3A%20%22http%3A%2F%2Fwww.example.com%2Fpath%22%2C%20%22countryCode%22%3A%20%22US%22%2C%20%22country%22%3A%20%22United%20State%22%2C%20%22region%22%3A%20%22CA%22%2C%20%22chat_feedback%22%3A%20%7B%22overall_chat%22%3A%205%2C%20%22responsiveness%22%3A%205%2C%20%22friendliness%22%3A%205%2C%20%22knowledge%22%3A%205%2C%20%22comments%22%3A%20%22Very%20helpful%2C%20thanks%22%7D%2C%20%22operatingSystem%22%3A%20%22Windows%22%2C%20%22emailAddress%22%3A%20%22bob%40example.com%22%2C%20%22organization%22%3A%20%22Widgets%20Inc.%22%2C%20%22phoneNumber%22%3A%20%22%28555%29%20555-5555%22%2C%20%22fullName%22%3A%20%22Bob%20Doe%22%2C%20%22customFields%22%3A%20%7B%22favoriteColor%22%3A%20%22blue%22%2C%20%22myInternalCustomerId%22%3A%20%2212341234%22%7D%2C%20%22id%22%3A%20%229QRF9YWM5XW3ZSU7P9CGWRU89944341%22%2C%20%22browser%22%3A%20%22Chrome%2012.1%22%7D%2C%20%22id%22%3A%20%22EV695BI2930A6XMO32886MPT899443414%22%7D"
    val payload = CollectorPayload(Shared.api, Nil, ContentType.some, body.some, Shared.cljSource, Shared.context)
    val expected = NonEmptyList("Olark event string failed to parse into JSON: [Unrecognized token 'kind': was expecting ('true', 'false' or 'null') at [Source: java.io.StringReader@xxxxxx; line: 1, column: 5]]")
    OlarkAdapter.toRawEvents(payload) must beFailing(expected)
  }
}
