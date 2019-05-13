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
package adapters.registry

import cats.data.{NonEmptyList, Validated}
import cats.syntax.either._
import cats.syntax.option._
import com.snowplowanalytics.snowplow.badrows._
import io.circe.literal._
import io.circe.parser._
import org.joda.time.DateTime
import org.specs2.matcher.ValidatedMatchers
import org.specs2.mutable.Specification

import loaders._
import utils.Clock._

class UrbanAirshipAdapterSpec extends Specification with ValidatedMatchers {

  object Shared {
    val api = CollectorPayload.Api("com.urbanairship.connect", "v1")
    val cljSource = CollectorPayload.Source("clj-tomcat", "UTF-8", None)
    val context = CollectorPayload.Context(
      None,
      "37.157.33.123".some,
      None,
      None,
      Nil,
      None
    ) // NB the collector timestamp is set to None!
  }

  "toRawEvents" should {

    val validPayload = json"""{
      "id": "e3314efb-9058-dbaf-c4bb-b754fca73613",
      "offset": "1",
      "occurred": "2015-11-13T16:31:52.393Z",
      "processed": "2015-11-13T16:31:52.393Z",
      "device": {
        "amazon_channel": "cd97c95c-ed77-f15a-3a67-5c2e26799d35"
      },
      "body": {
        "session_id": "27c75cab-a0b8-9da2-bc07-6d7253e0e13f"
      },
      "type": "CLOSE"
    }"""

    val invalidEvent = json"""{
      "id": "e3314efb-9058-dbaf-c4bb-b754fca73613",
      "offset": "1",
      "occurred": "2015-11-13T16:31:52.393Z",
      "processed": "2015-11-13T16:31:52.393Z",
      "device": {
        "amazon_channel": "cd97c95c-ed77-f15a-3a67-5c2e26799d35"
      },
      "body": {
        "session_id": "27c75cab-a0b8-9da2-bc07-6d7253e0e13f"
      },
      "type": "NOT_AN_EVENT_TYPE"
    }"""

    val payload = CollectorPayload(
      Shared.api,
      Nil,
      None,
      validPayload.noSpaces.some,
      Shared.cljSource,
      Shared.context
    )
    val actual = UrbanAirshipAdapter.toRawEvents(payload, SpecHelpers.client).value

    val expectedUnstructEventJson = json"""{
      "schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0",
      "data":{
        "schema":"iglu:com.urbanairship.connect/CLOSE/jsonschema/1-0-0",
        "data":{
            "id": "e3314efb-9058-dbaf-c4bb-b754fca73613",
            "offset": "1",
            "occurred": "2015-11-13T16:31:52.393Z",
            "processed": "2015-11-13T16:31:52.393Z",
            "device": {
                "amazon_channel": "cd97c95c-ed77-f15a-3a67-5c2e26799d35"
            },
            "body": {
              "session_id": "27c75cab-a0b8-9da2-bc07-6d7253e0e13f"
            },
            "type": "CLOSE"
        }
      }
    }"""

    "return the correct number of events (1)" in {
      actual must beValid
      val items = actual.toList.head.toList
      items must have size 1
    }

    "link to the correct json schema for the event type" in {
      actual must beValid
      val correctType = validPayload.hcursor.get[String]("type")
      correctType must be equalTo (Right("CLOSE"))

      val items = actual.toList.head.toList
      val sentSchema = parse(items.head.parameters("ue_pr"))
        .leftMap(_.getMessage)
        .flatMap(_.hcursor.downField("data").get[String]("schema").leftMap(_.getMessage))
      sentSchema must beRight("""iglu:com.urbanairship.connect/CLOSE/jsonschema/1-0-0""")
    }

    "fail on unknown event types" in {
      val payload = CollectorPayload(
        Shared.api,
        Nil,
        None,
        invalidEvent.noSpaces.some,
        Shared.cljSource,
        Shared.context
      )
      UrbanAirshipAdapter.toRawEvents(payload, SpecHelpers.client).value must beInvalid
    }

    "reject unparsable json" in {
      val payload =
        CollectorPayload(Shared.api, Nil, None, """{ """.some, Shared.cljSource, Shared.context)
      UrbanAirshipAdapter.toRawEvents(payload, SpecHelpers.client).value must beInvalid
    }

    "reject badly formatted json" in {
      val payload =
        CollectorPayload(
          Shared.api,
          Nil,
          None,
          """{ "value": "str" }""".some,
          Shared.cljSource,
          Shared.context
        )
      UrbanAirshipAdapter.toRawEvents(payload, SpecHelpers.client).value must beInvalid
    }

    "reject content types" in {
      val payload = CollectorPayload(
        Shared.api,
        Nil,
        "a/type".some,
        validPayload.noSpaces.some,
        Shared.cljSource,
        Shared.context
      )
      val res = UrbanAirshipAdapter.toRawEvents(payload, SpecHelpers.client).value

      res must beInvalid(
        NonEmptyList.one(
          FailureDetails.AdapterFailure
            .InputData("contentType", "a/type".some, "expected no content type")
        )
      )
    }

    "populate content-type as None (it's not applicable)" in {
      val contentType = actual.getOrElse(throw new IllegalStateException).head.contentType
      contentType must beEqualTo(None)
    }

    "have the correct collector source" in {
      val source = actual.getOrElse(throw new IllegalStateException).head.source
      source must beEqualTo(Shared.cljSource)
    }

    "have the correct context, including setting the correct collector timestamp" in {
      val context = actual.getOrElse(throw new IllegalStateException).head.context
      Shared.context.timestamp mustEqual None
      context mustEqual Shared.context.copy(
        timestamp = DateTime
          .parse("2015-11-13T16:31:52.393Z")
          .some
      ) // it should be set to the "processed" field by the adapter
    }

    "return the correct unstruct_event json" in {
      actual match {
        case Validated.Valid(successes) =>
          val event = successes.head
          parse(event.parameters("ue_pr")) must beRight(expectedUnstructEventJson)
        case _ => ko("payload was not accepted")
      }
    }

    "correctly populate the true timestamp" in {
      actual match {
        case Validated.Valid(successes) =>
          val event = successes.head
          // "occurred" field value in ms past epoch (2015-11-13T16:31:52.393Z)
          event.parameters("ttm") must beEqualTo("1447432312393")
        case _ => ko("payload was not populated")
      }
    }

    "correctly populate the eid" in {
      actual match {
        case Validated.Valid(successes) =>
          val event = successes.head
          // id field value
          event.parameters("eid") must beEqualTo("e3314efb-9058-dbaf-c4bb-b754fca73613")
        case _ => ko("payload was not populated")
      }
    }

  }

}
