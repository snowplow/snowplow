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
package good

// Scala
import com.snowplowanalytics.snowplow.enrich.hadoop.JobSpecHelpers

import scala.collection.mutable.{ArrayBuffer, ListBuffer, Buffer}

// Specs2
import org.specs2.mutable.Specification

// Scalding
import com.twitter.scalding._

// Cascading
import cascading.tuple.TupleEntry

// This project
import JobSpecHelpers._

// json4s
import org.json4s._
import org.json4s.jackson.JsonMethods._

/**
 * Holds the input and expected data
 * for the test.
 */
object NdjsonUrbanAirshipSingleEvent {

  val lines = Lines(compact(
                      parse("""
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
                               |""".stripMargin)
                  )
  )

  val expected = List(
    null,
    "srv",
    EtlTimestamp,
    "2015-11-13 16:31:52.393",
    null,
    "unstruct",
    null, // We can't predict the event_id
    null,
    null, // No tracker namespace
    "com.urbanairship.connect-v1",
    "ndjson",
    EtlVersion,
    null, // No user_id set
    null, // ip address not available
    null, // no fingerprint
    null, // no domain userid
    null, // no session index
    null, // no network userid
    null, // No geo-location for this IP address
    null,
    null,
    null,
    null,
    null,
    null,
    null, // No additional MaxMind databases used
    null,
    null,
    null,
    null, // no page_url
    null, // no page_title
    null, // no page_referrer
    null, // no page_urlscheme
    null, // no page_urlhost
    null, // no page_urlport
    null, // no page_urlpath
    null,
    null,
    null, // no refr_urlscheme
    null, // no refr_urlhost
    null, // no refr_urlport
    null, // no refr_urlpath
    null, // no refr_urlquery
    null,
    null, // no refr_medium
    null, // no refr_source
    null,
    null, // Marketing campaign fields empty
    null, //
    null, //
    null, //
    null, //
    null, // No custom contexts
    null, // Structured event fields empty
    null, //
    null, //
    null, //
    null, //
    compact(parse("""|{
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
    """.stripMargin)),
    null, // Transaction fields empty
    null, //
    null, //
    null, //
    null, //
    null, //
    null, //
    null, //
    null, // Transaction item fields empty
    null, //
    null, //
    null, //
    null, //
    null, //
    null, // Page ping fields empty
    null, //
    null, //
    null, //
    null, // no useragent
    null, // no br_name
    null, // no br_family
    null, // no br_version
    null, // no br_type
    null, // no br_renderengine
    null, // no br_lang
    null, // br_features_pdf not relevant
    null, // br_features_flash
    null, // br_features_java
    null, // br_features_director
    null, // br_features_quicktime
    null, // br_features_realplayer
    null, // br_features_windowsmedia
    null, // br_features_gears
    null, // br_features_silverlight
    null, // br_cookies
    null, // br_colordepth
    null, // br_viewwidth
    null, // br_viewheight
    null, // os_name
    null, // os_family
    null, // os_manufacturer
    null, // os_timezone
    null, // dvce_type
    null, // dvce_ismobile
    null, // dvce_screenwidth
    null, // dvce_screenheight
    null, // doc_charset
    null, // doc_width
    null  // doc_height
  )
}

/**
 * Multiple events and expected data
 */
object NdjsonUrbanAirshipMultiEvent {

  val sampleLine = compact(
    parse(
      """
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
        | """.stripMargin
    )
  )

  val sampleLineResponse = compact(
    parse(
      """|{
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
    )
  )

  val sampleBlank = "\r\n"

  val sampleInAppResolutionEvent = compact(
    parse(
      """{
        |  "id": "86604c72-4b29-5501-200a-4dc965738baf",
        |  "offset": "137",
        |  "occurred": "2015-11-13T16:34:08.394Z",
        |  "processed": "2015-11-13T16:31:52.393Z",
        |  "device": {
        |    "ios_channel": "3c58b101-6508-b0d6-8d3c-e5e87b75b193",
        |    "named_user_id": "3786888c-1fb9-a5b4-15db-d6a049333081"
        |  },
        |  "body": {
        |    "push_id": "cc978f41-4494-2836-8672-b9fb5c9de2e4",
        |    "type": "USER_DISMISSED",
        |    "duration": 9738
        |  },
        |  "type": "IN_APP_MESSAGE_RESOLUTION"
        |}""".stripMargin
    )
  )

  val sampleInAppResolutionEventResponse =  compact(
    parse(
      """{
        |  "schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0",
        |  "data":{
        |    "schema":"iglu:com.urbanairship.connect/IN_APP_MESSAGE_RESOLUTION/jsonschema/1-0-0",
        |    "data":{
        |           "id": "86604c72-4b29-5501-200a-4dc965738baf",
        |           "offset": "137",
        |           "occurred": "2015-11-13T16:34:08.394Z",
        |           "processed": "2015-11-13T16:31:52.393Z",
        |           "device": {
        |             "ios_channel": "3c58b101-6508-b0d6-8d3c-e5e87b75b193",
        |             "named_user_id": "3786888c-1fb9-a5b4-15db-d6a049333081"
        |           },
        |           "body": {
        |             "push_id": "cc978f41-4494-2836-8672-b9fb5c9de2e4",
        |             "type": "USER_DISMISSED",
        |             "duration": 9738
        |           },
        |           "type": "IN_APP_MESSAGE_RESOLUTION"
        |   }
        |  }
        |}""".stripMargin
    )
  )

  val eventSource = "srv"
  val collectorTstamp = "2015-11-13 16:31:52.393"
  val eventType = "unstruct"
  val adapter = "com.urbanairship.connect-v1"
  val loaderType = "ndjson"

  val expectedBase = {
    val r = ArrayBuffer.fill(NdjsonUrbanAirshipSingleEvent.expected.size)(null:String)
    r(1)  = eventSource
    r(2)  = EtlTimestamp
    r(3)  = collectorTstamp
    r(5)  = eventType
    r(9)  = adapter
    r(10) = loaderType
    r(11) = EtlVersion
    r.toList
  }

  val lines = Lines(sampleLine,
                    sampleBlank,
                    sampleInAppResolutionEvent,
                    sampleBlank,
                    sampleBlank) // the blanks should be ignored

  val expectedJsonOutputIdx = 58 // position of unstruct event json in list
  val expected = List(expectedBase.updated(expectedJsonOutputIdx, sampleLineResponse),
                      expectedBase.updated(expectedJsonOutputIdx, sampleInAppResolutionEventResponse))
}

/**
 * Integration test for the EtlJob:
 *
 * Check that all NDJSON lines are loaded and run through with the urbanairship adapter
 */
class NdjsonUrbanAirshipSpec extends Specification {

  "A job which processes a NDJSON file with one event" should {
    EtlJobSpec("ndjson/com.urbanairship.connect/v1", "2", true, List("geo")).
      source(MultipleTextLineFiles("inputFolder"), NdjsonUrbanAirshipSingleEvent.lines).
      sink[TupleEntry](Tsv("outputFolder")) { buf: Buffer[TupleEntry] =>
      "correctly output 1 event" in {
        buf.size must_== 1
        val actual = buf.head
        for (idx <- NdjsonUrbanAirshipSingleEvent.expected.indices) {
          actual.getString(idx) must beFieldEqualTo(NdjsonUrbanAirshipSingleEvent.expected(idx), withIndex = idx)
        }
      }
    }.
      sink[TupleEntry](Tsv("exceptionsFolder")) { trap =>
      "not trap any exceptions" in {
        trap must beEmpty
      }
    }.
      sink[String](Tsv("badFolder")) { error =>
      "not write any bad rows" in {
        error must beEmpty
      }
    }.
      run.
      finish
  }

  "A job which processes a NDJSON file with more than one event (but two valid ones)" should {
    EtlJobSpec("ndjson/com.urbanairship.connect/v1", "2", true, List("geo")).
      source(MultipleTextLineFiles("inputFolder"), NdjsonUrbanAirshipMultiEvent.lines).
      sink[TupleEntry](Tsv("outputFolder")) { buf: Buffer[TupleEntry] =>
      "correctly output 2 events" in {
        buf.size must_== 2

        buf.zipWithIndex foreach {
          case (actual, bufIdx) => {
            for (idx <- NdjsonUrbanAirshipMultiEvent.expected(bufIdx).indices) {
              actual.getString(idx) must beFieldEqualTo(NdjsonUrbanAirshipMultiEvent.expected(bufIdx)(idx), withIndex = idx)
            }
          }
        }

      }
    }.
      sink[TupleEntry](Tsv("exceptionsFolder")) { trap =>
      "not trap any exceptions" in {
        trap must beEmpty
      }
    }.
      sink[String](Tsv("badFolder")) { error =>
      "not write any bad rows" in {
        error must beEmpty
      }
    }.
      run.
      finish
  }
  
}
