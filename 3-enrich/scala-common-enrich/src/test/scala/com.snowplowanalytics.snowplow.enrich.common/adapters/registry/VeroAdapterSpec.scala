/*
 * Copyright (c) 2018 Snowplow Analytics Ltd. All rights reserved.
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
import org.joda.time.DateTime
import org.specs2.Specification
import org.specs2.matcher.{DataTables, ValidatedMatchers}

import loaders._
import outputs._
import utils.Clock._

class VeroAdapterSpec extends Specification with DataTables with ValidatedMatchers {
  def is = s2"""
  This is a specification to test the VeroAdapter functionality
  toRawEvents must return a success for a valid "sent" type payload body being passed                $e1
  toRawEvents must return a success for a valid "delivered" type payload body being passed           $e2
  toRawEvents must return a success for a valid "opened" type payload body being passed              $e3
  toRawEvents must return a success for a valid "clicked" type payload body being passed             $e4
  toRawEvents must return a success for a valid "bounced" type payload body being passed             $e5
  toRawEvents must return a success for a valid "unsubscribed" type payload body being passed        $e6
  toRawEvents must return a success for a valid "created" type payload body being passed             $e7
  toRawEvents must return a success for a valid "updated" type payload body being passed             $e8
  toRawEvents must return a Nel Success for a supported event type                                   $e9
  toRawEvents must return a Failure Nel if a body is not specified in the payload                    $e10
  """

  object Shared {
    val api = CollectorApi("com.getvero", "v1")
    val cljSource = CollectorSource("clj-tomcat", "UTF-8", None)
    val context = CollectorContext(
      DateTime.parse("2018-01-01T00:00:00.000+00:00").some,
      "37.157.33.123".some,
      None,
      None,
      Nil,
      None
    )
  }

  val ContentType = "application/json"

  def e1 = {
    val bodyStr =
      """{"sent_at": 1435016238, "event": {"name": "Test event", "triggered_at": 1424012238}, "type": "sent", "user": {"id": 123, "email": "steve@getvero.com"},"campaign": {"id": 987, "type": "transactional", "name": "Order confirmation", "subject": "Your order is being processed", "trigger-event": "purchased item", "permalink": "http://app.getvero.com/view/1/341d64944577ac1f70f560e37db54a25", "variation": "Variation A", "tags": "tag 1, tag 2"}}"""
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
          "tv" -> "com.getvero-v1",
          "e" -> "ue",
          "p" -> "srv",
          "ue_pr" -> """{"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0","data":{"schema":"iglu:com.getvero/sent/jsonschema/1-0-0","data":{"event":{"name":"Test event","triggered_at":"2015-02-15T14:57:18.000Z"},"sent_at":"2015-06-22T23:37:18.000Z","campaign":{"name":"Order confirmation","permalink":"http://app.getvero.com/view/1/341d64944577ac1f70f560e37db54a25","subject":"Your order is being processed","tags":"tag 1, tag 2","variation":"Variation A","trigger-event":"purchased item","id":987,"type":"transactional"},"user":{"id":123,"email":"steve@getvero.com"}}}}"""
        ),
        ContentType.some,
        Shared.cljSource,
        Shared.context
      )
    )
    VeroAdapter.toRawEvents(payload, SpecHelpers.client).value must beValid(expected)
  }

  def e2 = {
    val bodyStr =
      """{"delivered_at": 1435016238, "sender_ip": "127.0.0.1", "message_id": "20130920062934.21270.53268@vero.com", "event":{"name":"Test event","triggered_at":1424012238}, "type": "delivered", "user": {"id": 123, "email": "steve@getvero.com"},"campaign": {"id": 987, "type": "transactional", "name": "Order confirmation", "subject": "Your order is being processed", "trigger-event": "purchased item", "permalink": "http://app.getvero.com/view/1/341d64944577ac1f70f560e37db54a25", "variation": "Variation A", "tags": "tag 1, tag 2"}}"""
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
          "tv" -> "com.getvero-v1",
          "e" -> "ue",
          "p" -> "srv",
          "ue_pr" -> """{"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0","data":{"schema":"iglu:com.getvero/delivered/jsonschema/1-0-0","data":{"event":{"name":"Test event","triggered_at":"2015-02-15T14:57:18.000Z"},"campaign":{"name":"Order confirmation","permalink":"http://app.getvero.com/view/1/341d64944577ac1f70f560e37db54a25","subject":"Your order is being processed","tags":"tag 1, tag 2","variation":"Variation A","trigger-event":"purchased item","id":987,"type":"transactional"},"delivered_at":"2015-06-22T23:37:18.000Z","message_id":"20130920062934.21270.53268@vero.com","sender_ip":"127.0.0.1","user":{"id":123,"email":"steve@getvero.com"}}}}"""
        ),
        ContentType.some,
        Shared.cljSource,
        Shared.context
      )
    )
    VeroAdapter.toRawEvents(payload, SpecHelpers.client).value must beValid(expected)
  }

  def e3 = {
    val bodyStr =
      """{"opened_at": 1435016238, "user_agent":"Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0)", "message_id": "20130920062934.21270.53268@vero.com", "event": {"name": "Test event", "triggered_at": 1424012238}, "type": "opened", "user": {"id": 123, "email": "steve@getvero.com"},"campaign": {"id": 987, "type": "transactional", "name": "Order confirmation", "subject": "Your order is being processed", "trigger-event": "purchased item", "permalink": "http://app.getvero.com/view/1/341d64944577ac1f70f560e37db54a25", "variation": "Variation A", "tags": "tag 1, tag 2"}}"""
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
          "tv" -> "com.getvero-v1",
          "e" -> "ue",
          "p" -> "srv",
          "ue_pr" -> """{"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0","data":{"schema":"iglu:com.getvero/opened/jsonschema/1-0-0","data":{"opened_at":"2015-06-22T23:37:18.000Z","event":{"name":"Test event","triggered_at":"2015-02-15T14:57:18.000Z"},"campaign":{"name":"Order confirmation","permalink":"http://app.getvero.com/view/1/341d64944577ac1f70f560e37db54a25","subject":"Your order is being processed","tags":"tag 1, tag 2","variation":"Variation A","trigger-event":"purchased item","id":987,"type":"transactional"},"user_agent":"Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0)","message_id":"20130920062934.21270.53268@vero.com","user":{"id":123,"email":"steve@getvero.com"}}}}"""
        ),
        ContentType.some,
        Shared.cljSource,
        Shared.context
      )
    )
    VeroAdapter.toRawEvents(payload, SpecHelpers.client).value must beValid(expected)
  }

  def e4 = {
    val bodyStr =
      """{"clicked_at": 1435016238, "user_agent":"Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0)", "message_id": "20130920062934.21270.53268@vero.com", "event": {"name": "Test event", "triggered_at": 1424012238}, "type": "clicked", "user": {"id": 123, "email": "steve@getvero.com"},"campaign": {"id": 987, "type": "transactional", "name": "Order confirmation", "subject": "Your order is being processed", "trigger-event": "purchased item", "permalink": "http://app.getvero.com/view/1/341d64944577ac1f70f560e37db54a25", "variation": "Variation A", "tags": "tag 1, tag 2"}}"""
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
          "tv" -> "com.getvero-v1",
          "e" -> "ue",
          "p" -> "srv",
          "ue_pr" -> """{"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0","data":{"schema":"iglu:com.getvero/clicked/jsonschema/1-0-0","data":{"clicked_at":"2015-06-22T23:37:18.000Z","event":{"name":"Test event","triggered_at":"2015-02-15T14:57:18.000Z"},"campaign":{"name":"Order confirmation","permalink":"http://app.getvero.com/view/1/341d64944577ac1f70f560e37db54a25","subject":"Your order is being processed","tags":"tag 1, tag 2","variation":"Variation A","trigger-event":"purchased item","id":987,"type":"transactional"},"user_agent":"Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0)","message_id":"20130920062934.21270.53268@vero.com","user":{"id":123,"email":"steve@getvero.com"}}}}"""
        ),
        ContentType.some,
        Shared.cljSource,
        Shared.context
      )
    )
    VeroAdapter.toRawEvents(payload, SpecHelpers.client).value must beValid(expected)
  }

  def e5 = {
    val bodyStr =
      """{"bounced_at": 1435016238, "bounce_type":"hard", "bounce_code": "521", "bounce_message": "521 5.2.1 :  AOL will not accept delivery of this message.", "message_id": "20130920062934.21270.53268@vero.com", "event": {"name": "Test event", "triggered_at": 1424012238}, "type": "bounced", "user": {"id": 123, "email": "steve@getvero.com"},"campaign": {"id": 987, "type": "transactional", "name": "Order confirmation", "subject": "Your order is being processed", "trigger-event": "purchased item", "permalink": "http://app.getvero.com/view/1/341d64944577ac1f70f560e37db54a25", "variation": "Variation A"}}"""
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
          "tv" -> "com.getvero-v1",
          "e" -> "ue",
          "p" -> "srv",
          "ue_pr" -> """{"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0","data":{"schema":"iglu:com.getvero/bounced/jsonschema/1-0-0","data":{"bounce_message":"521 5.2.1 :  AOL will not accept delivery of this message.","event":{"name":"Test event","triggered_at":"2015-02-15T14:57:18.000Z"},"bounced_at":"2015-06-22T23:37:18.000Z","campaign":{"name":"Order confirmation","permalink":"http://app.getvero.com/view/1/341d64944577ac1f70f560e37db54a25","subject":"Your order is being processed","variation":"Variation A","trigger-event":"purchased item","id":987,"type":"transactional"},"message_id":"20130920062934.21270.53268@vero.com","bounce_type":"hard","bounce_code":"521","user":{"id":123,"email":"steve@getvero.com"}}}}"""
        ),
        ContentType.some,
        Shared.cljSource,
        Shared.context
      )
    )
    VeroAdapter.toRawEvents(payload, SpecHelpers.client).value must beValid(expected)
  }

  def e6 = {
    val bodyStr =
      """{"unsubscribed_at": 1435016238, "type": "unsubscribed", "user": {"id": 123, "email": "steve@getvero.com"}}"""
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
          "tv" -> "com.getvero-v1",
          "e" -> "ue",
          "p" -> "srv",
          "ue_pr" -> """{"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0","data":{"schema":"iglu:com.getvero/unsubscribed/jsonschema/1-0-0","data":{"unsubscribed_at":"2015-06-22T23:37:18.000Z","user":{"id":123,"email":"steve@getvero.com"}}}}"""
        ),
        ContentType.some,
        Shared.cljSource,
        Shared.context
      )
    )
    VeroAdapter.toRawEvents(payload, SpecHelpers.client).value must beValid(expected)
  }

  def e7 = {
    val bodyStr =
      """{"type": "user_created", "user": {"id": 123, "email": "steve@getvero.com"}, "firstname": "Steve", "company": "Vero", "role": "Bot"}"""
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
          "tv" -> "com.getvero-v1",
          "e" -> "ue",
          "p" -> "srv",
          "ue_pr" -> """{"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0","data":{"schema":"iglu:com.getvero/created/jsonschema/1-0-0","data":{"role":"Bot","firstname":"Steve","company":"Vero","user":{"id":123,"email":"steve@getvero.com"}}}}"""
        ),
        ContentType.some,
        Shared.cljSource,
        Shared.context
      )
    )
    VeroAdapter.toRawEvents(payload, SpecHelpers.client).value must beValid(expected)
  }

  def e8 = {
    val bodyStr =
      """{"type": "user_updated", "user": {"id": 123, "email": "steve@getvero.com"}, "changes": {"_tags": {"add": ["active-customer"], "remove": ["unactive-180-days"]}}}"""
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
          "tv" -> "com.getvero-v1",
          "e" -> "ue",
          "p" -> "srv",
          "ue_pr" -> """{"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0","data":{"schema":"iglu:com.getvero/updated/jsonschema/1-0-0","data":{"user":{"id":123,"email":"steve@getvero.com"},"changes":{"tags":{"add":["active-customer"],"remove":["unactive-180-days"]}}}}}"""
        ),
        ContentType.some,
        Shared.cljSource,
        Shared.context
      )
    )
    VeroAdapter.toRawEvents(payload, SpecHelpers.client).value must beValid(expected)
  }

  def e9 =
    "SPEC NAME" || "SCHEMA TYPE" | "EXPECTED SCHEMA" |
      "Valid, type sent" !! "sent" ! "iglu:com.getvero/sent/jsonschema/1-0-0" |
      "Valid, type unsubscribed" !! "unsubscribed" ! "iglu:com.getvero/unsubscribed/jsonschema/1-0-0" |
      "Valid, type delivered" !! "delivered" ! "iglu:com.getvero/delivered/jsonschema/1-0-0" |
      "Valid, type opened" !! "opened" ! "iglu:com.getvero/opened/jsonschema/1-0-0" |
      "Valid, type clicked" !! "clicked" ! "iglu:com.getvero/clicked/jsonschema/1-0-0" |
      "Valid, type created" !! "user_created" ! "iglu:com.getvero/created/jsonschema/1-0-0" |
      "Valid, type updated" !! "user_updated" ! "iglu:com.getvero/updated/jsonschema/1-0-0" |
      "Valid, type bounced" !! "bounced" ! "iglu:com.getvero/bounced/jsonschema/1-0-0" |> {
      (_, schema, expected) =>
        val body = "{\"type\":\"" + schema + "\"}"
        val payload = CollectorPayload(
          Shared.api,
          Nil,
          ContentType.some,
          body.some,
          Shared.cljSource,
          Shared.context
        )
        val expectedJson = "{\"schema\":\"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0\",\"data\":{\"schema\":\"" + expected + "\",\"data\":{}}}"
        val actual = VeroAdapter.toRawEvents(payload, SpecHelpers.client).value
        actual must beValid(
          NonEmptyList.one(
            RawEvent(
              Shared.api,
              Map("tv" -> "com.getvero-v1", "e" -> "ue", "p" -> "srv", "ue_pr" -> expectedJson),
              ContentType.some,
              Shared.cljSource,
              Shared.context
            )
          )
        )
    }

  def e10 = {
    val payload =
      CollectorPayload(Shared.api, Nil, ContentType.some, None, Shared.cljSource, Shared.context)
    VeroAdapter.toRawEvents(payload, SpecHelpers.client).value must beInvalid(
      NonEmptyList.one(InputDataAdapterFailure("body", None, "empty body: no events to process"))
    )
  }
}
