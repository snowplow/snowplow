/*
 * Copyright (c) 2015 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.enrich.common.adapters.registry

import com.snowplowanalytics.snowplow.enrich.common.SpecHelpers
import com.snowplowanalytics.snowplow.enrich.common.loaders.{CollectorApi, CollectorContext, CollectorPayload, CollectorSource}
import org.joda.time.DateTime

// Scalaz
import scalaz.Scalaz._
import scalaz._

// Specs2
import org.specs2.mutable.Specification
import org.specs2.scalaz.ValidationMatchers

// json4s
import org.json4s._
import org.json4s.jackson.JsonMethods._


class UrbanAirshipAdapterSpec extends Specification with ValidationMatchers {

  implicit val resolver = SpecHelpers.IgluResolver
  implicit val formats = DefaultFormats

  object Shared {
    val api = CollectorApi("com.urbanairship.connect", "v1")
    val cljSource = CollectorSource("clj-tomcat", "UTF-8", None)
    val context = CollectorContext(None, "37.157.33.123".some, None, None, Nil, None)  // NB the collector timestamp is set to None!
  }

  "toRawEvents" should {

    val validPayload = """
                         |{
                         |  "id": "e3314efb-9058-dbaf-c4bb-b754fca73613",
                         |  "offset": "1",
                         |  "occurred": "2015-11-13T16:31:52.393Z",
                         |  "processed": "2015-11-13T16:31:52.393Z",
                         |  "device": {
                         |    "amazon_channel": "cd97c95c-ed77-f15a-3a67-5c2e26799d35"
                         |  },
                         |  "body": {
                         |    "session_id": "27c75cab-a0b8-9da2-bc07-6d7253e0e13f"
                         |  },
                         |  "type": "CLOSE"
                         |}
                         |""".stripMargin

    val invalidEvent = """
                         |{
                         |  "id": "e3314efb-9058-dbaf-c4bb-b754fca73613",
                         |  "offset": "1",
                         |  "occurred": "2015-11-13T16:31:52.393Z",
                         |  "processed": "2015-11-13T16:31:52.393Z",
                         |  "device": {
                         |    "amazon_channel": "cd97c95c-ed77-f15a-3a67-5c2e26799d35"
                         |  },
                         |  "body": {
                         |    "session_id": "27c75cab-a0b8-9da2-bc07-6d7253e0e13f"
                         |  },
                         |  "type": "NOT_AN_EVENT_TYPE"
                         |}
                         |""".stripMargin

    val payload = CollectorPayload(Shared.api, Nil, None, validPayload.some, Shared.cljSource, Shared.context)
    val actual = UrbanAirshipAdapter.toRawEvents(payload)

    val expectedUnstructEventJson = """|{
                               |  "schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0",
                               |  "data":{
                               |    "schema":"iglu:com.urbanairship.connect/CLOSE/jsonschema/1-0-0",
                               |    "data":{
                               |        "id": "e3314efb-9058-dbaf-c4bb-b754fca73613",
                               |         "offset": "1",
                               |         "occurred": "2015-11-13T16:31:52.393Z",
                               |         "processed": "2015-11-13T16:31:52.393Z",
                               |         "device": {
                               |             "amazon_channel": "cd97c95c-ed77-f15a-3a67-5c2e26799d35"
                               |         },
                               |         "body": {
                               |           "session_id": "27c75cab-a0b8-9da2-bc07-6d7253e0e13f"
                               |         },
                               |         "type": "CLOSE"
                               |    }
                               |  }
                               |}
                             """.stripMargin

    val expectedCompactedUnstructEvent = compact(parse(expectedUnstructEventJson))

    "return the correct number of events (1)" in {
      actual must beSuccessful
      val items = actual.toList.head.toList
      items must have size 1
    }

    "link to the correct json schema for the event type" in {
      actual must beSuccessful
      val correctType = (parse(validPayload) \ "type").extract[String]
      correctType must be equalTo ("CLOSE")

      val items = actual.toList.head.toList
      val sentSchema = (parse(items.head.parameters("ue_pr")) \ "data") \ "schema"
      sentSchema.extract[String] must beEqualTo("""iglu:com.urbanairship.connect/CLOSE/jsonschema/1-0-0""")
    }

    "fail on unknown event types" in {
      val payload = CollectorPayload(Shared.api, Nil, None, invalidEvent.some, Shared.cljSource, Shared.context)
      UrbanAirshipAdapter.toRawEvents(payload) must beFailing
    }

    "reject unparsable json" in {
      val payload = CollectorPayload(Shared.api, Nil, None, """{ """.some, Shared.cljSource, Shared.context)
      UrbanAirshipAdapter.toRawEvents(payload) must beFailing
    }

    "reject badly formatted json" in {
      val payload = CollectorPayload(Shared.api, Nil, None, """{ "value": "str" }""".some, Shared.cljSource, Shared.context)
      UrbanAirshipAdapter.toRawEvents(payload) must beFailing
    }

    "reject content types" in {
      val payload = CollectorPayload(Shared.api, Nil, "a/type".some, validPayload.some, Shared.cljSource, Shared.context)
      val res = UrbanAirshipAdapter.toRawEvents(payload)

      res must beFailing(NonEmptyList("Content type of a/type provided, expected None for UrbanAirship"))
    }

    "populate content-type as None (it's not applicable)" in {
      val contentType = actual.getOrElse(throw new IllegalStateException).head.contentType
      contentType must beEqualTo(None)
    }

    "have the correct collector source" in {
      val source = actual.getOrElse(throw new IllegalStateException).head.source
      source must beEqualTo (Shared.cljSource)
    }

    "have the correct context, including setting the correct collector timestamp" in {
      val context = actual.getOrElse(throw new IllegalStateException).head.context
      Shared.context.timestamp mustEqual None
      context mustEqual Shared.context.copy(timestamp = DateTime.parse("2015-11-13T16:31:52.393Z").some) // it should be set to the "processed" field by the adapter
    }

    "return the correct unstruct_event json" in {
      actual match {
        case Success(successes) => {
          val event = successes.head
          compact(parse(event.parameters("ue_pr"))) must beEqualTo(expectedCompactedUnstructEvent)
        }
        case _ => ko("payload was not accepted")
      }
    }

    "correctly populate the true timestamp" in {
      actual match {
        case Success(successes) => {
          val event = successes.head
          event.parameters("ttm") must beEqualTo("1447432312393") // "occurred" field value in ms past epoch (2015-11-13T16:31:52.393Z)
        }
        case _ => ko("payload was not populated")
      }
    }

    "correctly populate the eid" in {
      actual match {
        case Success(successes) => {
          val event = successes.head
          event.parameters("eid") must beEqualTo("e3314efb-9058-dbaf-c4bb-b754fca73613") // id field value
        }
        case _ => ko("payload was not populated")
      }
    }

  }

}
