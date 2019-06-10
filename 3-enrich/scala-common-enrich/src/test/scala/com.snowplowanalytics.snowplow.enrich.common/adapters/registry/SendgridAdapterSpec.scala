/*
 * Copyright (c) 2015-2019 Snowplow Analytics Ltd. All rights reserved.
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
import com.snowplowanalytics.snowplow.badrows.AdapterFailure._
import com.snowplowanalytics.snowplow.badrows.Payload.{CollectorPayload => _}
import org.joda.time.DateTime
import org.specs2.matcher.ValidatedMatchers
import org.specs2.mutable.Specification

import loaders._
import utils.Clock._

class SendgridAdapterSpec extends Specification with ValidatedMatchers {
  object Shared {
    val api = CollectorApi("com.sendgrid", "v3")
    val cljSource = CollectorSource("clj-tomcat", "UTF-8", None)
    val context = CollectorContext(
      DateTime.parse("2013-08-29T00:18:48.000+00:00").some,
      "37.157.33.123".some,
      None,
      None,
      Nil,
      None
    )
  }

  val ContentType = "application/json"

  // this could do with going somewhere else
  val samplePostPayload =
    """
[
   {
      "email":"example@test.com",
      "timestamp":1446549615,
      "smtp-id":"\u003c14c5d75ce93.dfd.64b469@ismtpd-555\u003e",
      "event":"processed",
      "category":"cat facts",
      "sg_event_id":"sZROwMGMagFgnOEmSdvhig==",
      "sg_message_id":"14c5d75ce93.dfd.64b469.filter0001.16648.5515E0B88.0",
      "marketing_campaign_id":12345,
      "marketing_campaign_name":"campaign name",
      "marketing_campaign_version":"B",
      "marketing_campaign_split_id":13471
   },
   {
      "email":"example@test.com",
      "timestamp":1446549615,
      "smtp-id":"\u003c14c5d75ce93.dfd.64b469@ismtpd-555\u003e",
      "event":"deferred",
      "category":"cat facts",
      "sg_event_id":"jWmZXTZbtHTV2-S47asrww==",
      "sg_message_id":"14c5d75ce93.dfd.64b469.filter0001.16648.5515E0B88.0",
      "marketing_campaign_id":12345,
      "marketing_campaign_name":"campaign name",
      "marketing_campaign_version":"B",
      "marketing_campaign_split_id":13471,
      "response":"400 try again later",
      "attempt":"5"
   },
   {
      "email":"example@test.com",
      "timestamp":1446549615,
      "smtp-id":"\u003c14c5d75ce93.dfd.64b469@ismtpd-555\u003e",
      "event":"delivered",
      "category":"cat facts",
      "sg_event_id":"cikAODhD-ffTphZ7xixsRw==",
      "sg_message_id":"14c5d75ce93.dfd.64b469.filter0001.16648.5515E0B88.0",
      "marketing_campaign_id":12345,
      "marketing_campaign_name":"campaign name",
      "marketing_campaign_version":"B",
      "marketing_campaign_split_id":13471,
      "response":"250 OK"
   },
   {
      "email":"example@test.com",
      "timestamp":1446549615,
      "smtp-id":"\u003c14c5d75ce93.dfd.64b469@ismtpd-555\u003e",
      "event":"open",
      "category":"cat facts",
      "sg_event_id":"VGRrZCh-qMkOaAmuxcFujA==",
      "sg_message_id":"14c5d75ce93.dfd.64b469.filter0001.16648.5515E0B88.0",
      "marketing_campaign_id":12345,
      "marketing_campaign_name":"campaign name",
      "marketing_campaign_version":"B",
      "marketing_campaign_split_id":13471,
      "useragent":"Mozilla/4.0 (compatible; MSIE 6.1; Windows XP; .NET CLR 1.1.4322; .NET CLR 2.0.50727)",
      "ip":"255.255.255.255"
   },
   {
      "email":"example@test.com",
      "timestamp":1446549615,
      "smtp-id":"\u003c14c5d75ce93.dfd.64b469@ismtpd-555\u003e",
      "event":"click",
      "category":"cat facts",
      "sg_event_id":"QjGWYpcksoD31aVQAONfAg==",
      "sg_message_id":"14c5d75ce93.dfd.64b469.filter0001.16648.5515E0B88.0",
      "marketing_campaign_id":12345,
      "marketing_campaign_name":"campaign name",
      "marketing_campaign_version":"B",
      "marketing_campaign_split_id":13471,
      "useragent":"Mozilla/4.0 (compatible; MSIE 6.1; Windows XP; .NET CLR 1.1.4322; .NET CLR 2.0.50727)",
      "ip":"255.255.255.255",
      "url":"http://www.sendgrid.com/"
   },
   {
      "email":"example@test.com",
      "timestamp":1446549615,
      "smtp-id":"\u003c14c5d75ce93.dfd.64b469@ismtpd-555\u003e",
      "event":"bounce",
      "category":"cat facts",
      "sg_event_id":"PQmsSRnaTMVde4mu4TUgTQ==",
      "sg_message_id":"14c5d75ce93.dfd.64b469.filter0001.16648.5515E0B88.0",
      "marketing_campaign_id":12345,
      "marketing_campaign_name":"campaign name",
      "marketing_campaign_version":"B",
      "marketing_campaign_split_id":13471,
      "reason":"500 unknown recipient",
      "status":"5.0.0"
   },
   {
      "email":"example@test.com",
      "timestamp":1446549615,
      "smtp-id":"\u003c14c5d75ce93.dfd.64b469@ismtpd-555\u003e",
      "event":"dropped",
      "category":"cat facts",
      "sg_event_id":"BP0-vnv2BjDPzwaldo-XVg==",
      "sg_message_id":"14c5d75ce93.dfd.64b469.filter0001.16648.5515E0B88.0",
      "marketing_campaign_id":12345,
      "marketing_campaign_name":"campaign name",
      "marketing_campaign_version":"B",
      "marketing_campaign_split_id":13471,
      "reason":"Bounced Address",
      "status":"5.0.0"
   },
   {
      "email":"example@test.com",
      "timestamp":1446549615,
      "smtp-id":"\u003c14c5d75ce93.dfd.64b469@ismtpd-555\u003e",
      "event":"spamreport",
      "category":"cat facts",
      "sg_event_id":"ApWZolLiPe04wm5jAhFifA==",
      "sg_message_id":"14c5d75ce93.dfd.64b469.filter0001.16648.5515E0B88.0",
      "marketing_campaign_id":12345,
      "marketing_campaign_name":"campaign name",
      "marketing_campaign_version":"B",
      "marketing_campaign_split_id":13471
   },
   {
      "email":"example@test.com",
      "timestamp":1446549615,
      "smtp-id":"\u003c14c5d75ce93.dfd.64b469@ismtpd-555\u003e",
      "event":"unsubscribe",
      "category":"cat facts",
      "sg_event_id":"HoBsy5C1Tcoc1dJNsy5SfA==",
      "sg_message_id":"14c5d75ce93.dfd.64b469.filter0001.16648.5515E0B88.0",
      "marketing_campaign_id":12345,
      "marketing_campaign_name":"campaign name",
      "marketing_campaign_version":"B",
      "marketing_campaign_split_id":13471
   },
   {
      "email":"example@test.com",
      "timestamp":1446549615,
      "smtp-id":"\u003c14c5d75ce93.dfd.64b469@ismtpd-555\u003e",
      "event":"group_unsubscribe",
      "category":"cat facts",
      "sg_event_id":"hew55AFBIgLbd33pcviQTQ==",
      "sg_message_id":"14c5d75ce93.dfd.64b469.filter0001.16648.5515E0B88.0",
      "marketing_campaign_id":12345,
      "marketing_campaign_name":"campaign name",
      "marketing_campaign_version":"B",
      "marketing_campaign_split_id":13471,
      "useragent":"Mozilla/4.0 (compatible; MSIE 6.1; Windows XP; .NET CLR 1.1.4322; .NET CLR 2.0.50727)",
      "ip":"255.255.255.255",
      "url":"http://www.sendgrid.com/",
      "asm_group_id":10
   },
   {
      "email":"example@test.com",
      "timestamp":1446549615,
      "smtp-id":"\u003c14c5d75ce93.dfd.64b469@ismtpd-555\u003e",
      "event":"group_resubscribe",
      "category":"cat facts",
      "sg_event_id":"TDlqEy7cUfKLVMY3EAVCag==",
      "sg_message_id":"14c5d75ce93.dfd.64b469.filter0001.16648.5515E0B88.0",
      "marketing_campaign_id":12345,
      "marketing_campaign_name":"campaign name",
      "marketing_campaign_version":"B",
      "marketing_campaign_split_id":13471,
      "useragent":"Mozilla/4.0 (compatible; MSIE 6.1; Windows XP; .NET CLR 1.1.4322; .NET CLR 2.0.50727)",
      "ip":"255.255.255.255",
      "url":"http://www.sendgrid.com/",
      "asm_group_id":10
   }
]
    """

  "toRawEvents" should {

    val payload =
      CollectorPayload(
        Shared.api,
        Nil,
        ContentType.some,
        samplePostPayload.some,
        Shared.cljSource,
        Shared.context
      )
    val actual = SendgridAdapter.toRawEvents(payload, SpecHelpers.client).value

    "return the correct number of events" in {
      actual must beValid
      val items = actual.toList.head.toList

      items must have size 11
    }

    "have the correct api endpoint for each element" in {
      actual must beValid
      val items = actual.toList.head.toList
      val siz = items.count(itm => itm.api == Shared.api)

      siz must beEqualTo(items.size)
    }

    "have the correct content type for each element" in {
      actual must beValid
      val items = actual.toList.head.toList
      val siz = items.count(itm => itm.contentType.get == ContentType)

      siz must beEqualTo(items.toList.size)
    }

    "have the correct source for each element" in {
      actual must beValid
      val items = actual.toList.head.toList
      val siz = items.count(itm => itm.source == Shared.cljSource)

      siz must beEqualTo(items.toList.size)
    }

    "have the correct context for each element" in {
      actual must beValid
      val items = actual.toList.head.toList
      val siz = items.count(itm => itm.context == Shared.context)

      siz must beEqualTo(items.toList.size)
    }

    "reject empty bodies" in {
      val invalidpayload =
        CollectorPayload(Shared.api, Nil, ContentType.some, None, Shared.cljSource, Shared.context)
      val toBeRejected = SendgridAdapter.toRawEvents(invalidpayload, SpecHelpers.client).value

      toBeRejected must beInvalid
    }

    "reject empty content type" in {
      val invalidpayload =
        CollectorPayload(
          Shared.api,
          Nil,
          None,
          samplePostPayload.some,
          Shared.cljSource,
          Shared.context
        )
      val toBeRejected = SendgridAdapter.toRawEvents(invalidpayload, SpecHelpers.client).value
      toBeRejected must beInvalid
    }

    "reject unexpected content type" in {
      val invalidpayload =
        CollectorPayload(
          Shared.api,
          Nil,
          "invalidtype/invalid".some,
          samplePostPayload.some,
          Shared.cljSource,
          Shared.context
        )
      SendgridAdapter.toRawEvents(invalidpayload, SpecHelpers.client).value must beInvalid
    }

    "accept content types with explicit charsets" in {
      val payload =
        CollectorPayload(
          Shared.api,
          Nil,
          "application/json; charset=utf-8".some,
          samplePostPayload.some,
          Shared.cljSource,
          Shared.context
        )
      val res = SendgridAdapter.toRawEvents(payload, SpecHelpers.client).value
      res must beValid
    }

    "reject unsupported event types" in {

      val invalidEventTypeJson =
        """
            [
               {
                 "email": "example@test.com",
                 "timestamp": 1446549615,
                 "smtp-id": "\u003c14c5d75ce93.dfd.64b469@ismtpd-555\u003e",
                 "event": "moon landing",
                 "category": "cat facts",
                 "sg_event_id": "sZROwMGMagFgnOEmSdvhig==",
                 "sg_message_id": "14c5d75ce93.dfd.64b469.filter0001.16648.5515E0B88.0"
                }
            ]"""

      val invalidpayload =
        CollectorPayload(
          Shared.api,
          Nil,
          ContentType.some,
          invalidEventTypeJson.some,
          Shared.cljSource,
          Shared.context
        )

      SendgridAdapter.toRawEvents(invalidpayload, SpecHelpers.client).value must beInvalid
    }

    "reject invalid/unparsable json" in {
      val unparsableJson = """[ """
      SendgridAdapter
        .toRawEvents(
          CollectorPayload(
            Shared.api,
            Nil,
            ContentType.some,
            unparsableJson.some,
            Shared.cljSource,
            Shared.context
          ),
          SpecHelpers.client
        )
        .value must beInvalid
    }

    "reject valid json in incorrect format" in {
      val incorrectlyFormattedJson = """[ ]"""
      SendgridAdapter
        .toRawEvents(
          CollectorPayload(
            Shared.api,
            Nil,
            ContentType.some,
            incorrectlyFormattedJson.some,
            Shared.cljSource,
            Shared.context
          ),
          SpecHelpers.client
        )
        .value must beInvalid
    }

    "reject a payload with a some valid, some invalid events" in {
      val missingEventType =
        """
      [
         {
           "email": "example@test.com",
           "timestamp": 1446549615,
           "smtp-id": "\u003c14c5d75ce93.dfd.64b469@ismtpd-555\u003e",
           "event": "processed",
           "category": "cat facts",
           "sg_event_id": "sZROwMGMagFgnOEmSdvhig==",
           "sg_message_id": "14c5d75ce93.dfd.64b469.filter0001.16648.5515E0B88.0"
          },
          {
           "email": "example@test.com",
           "timestamp": 1446549615,
           "smtp-id": "\u003c14c5d75ce93.dfd.64b469@ismtpd-555\u003e",
           "category": "cat facts",
           "sg_event_id": "sZROwMGMagFgnOEmSdvhig==",
           "sg_message_id": "14c5d75ce93.dfd.64b469.filter0001.16648.5515E0B88.0"
          }
      ]"""

      val payload =
        CollectorPayload(
          Shared.api,
          Nil,
          ContentType.some,
          missingEventType.some,
          Shared.cljSource,
          Shared.context
        )
      val actual = SendgridAdapter.toRawEvents(payload, SpecHelpers.client).value
      actual must beInvalid(
        NonEmptyList.one(
          SchemaMappingAdapterFailure(
            None,
            SendgridAdapter.EventSchemaMap,
            "cannot determine event type: type parameter not provided at index 1"
          )
        )
      )
    }

    "return correct json for sample event, including stripping out event keypair and fixing timestamp" in {

      val inputJson =
        """
      [
         {
           "email": "example@test.com",
           "timestamp": 1446549615,
           "smtp-id": "\u003c14c5d75ce93.dfd.64b469@ismtpd-555\u003e",
           "event": "processed",
           "category": "cat facts",
           "sg_event_id": "sZROwMGMagFgnOEmSdvhig==",
           "sg_message_id": "14c5d75ce93.dfd.64b469.filter0001.16648.5515E0B88.0",
           "marketing_campaign_id":12345,
           "marketing_campaign_name":"campaign name",
           "marketing_campaign_version":"B",
           "marketing_campaign_split_id":13471
          }
      ]"""

      val payload =
        CollectorPayload(
          Shared.api,
          Nil,
          ContentType.some,
          inputJson.some,
          Shared.cljSource,
          Shared.context
        )

      val expectedJson =
        """{"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0","data":{"schema":"iglu:com.sendgrid/processed/jsonschema/2-0-0","data":{"email":"example@test.com","timestamp":"2015-11-03T11:20:15.000Z","smtp-id":"\u003c14c5d75ce93.dfd.64b469@ismtpd-555\u003e","category":"cat facts","sg_event_id":"sZROwMGMagFgnOEmSdvhig==","sg_message_id":"14c5d75ce93.dfd.64b469.filter0001.16648.5515E0B88.0","marketing_campaign_id":12345,"marketing_campaign_name":"campaign name","marketing_campaign_version":"B","marketing_campaign_split_id":13471}}}}"""

      val actual = SendgridAdapter.toRawEvents(payload, SpecHelpers.client).value
      actual must beValid(
        NonEmptyList.one(
          RawEvent(
            Shared.api,
            Map(
              "tv" -> "com.sendgrid-v3",
              "e" -> "ue",
              "p" -> "srv",
              "ue_pr" -> expectedJson // NB this includes removing the "event" keypair as redundant
            ),
            ContentType.some,
            Shared.cljSource,
            Shared.context
          )
        )
      )
    }
  }
}
