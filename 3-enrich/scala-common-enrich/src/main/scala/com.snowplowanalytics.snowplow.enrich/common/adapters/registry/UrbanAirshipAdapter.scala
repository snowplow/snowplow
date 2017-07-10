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
package com.snowplowanalytics
package snowplow
package enrich
package common
package adapters
package registry

// Java
import com.fasterxml.jackson.core.JsonParseException

// Scalaz
import scalaz.Scalaz._

// json4s
import org.json4s._
import org.json4s.jackson.JsonMethods._

// Iglu
import com.snowplowanalytics.iglu.client.{Resolver, SchemaKey}

// Joda Time
import org.joda.time.{DateTime, DateTimeZone}
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}

// This project
import com.snowplowanalytics.snowplow.enrich.common.loaders.CollectorPayload
import com.snowplowanalytics.snowplow.enrich.common.utils.{JsonUtils => JU}

/**
 * Transforms a collector payload which conforms to
 * a known version of the UrbanAirship Connect API
 * into raw events.
 */
object UrbanAirshipAdapter extends Adapter {

  // Vendor name for Failure Message
  private val VendorName = "UrbanAirship"

  // Tracker version for an UrbanAirship Connect API
  private val TrackerVersion = "com.urbanairship.connect-v1"

  // Schemas for reverse-engineering a Snowplow unstructured event
  private val EventSchemaMap = Map (
    "CLOSE"                         -> SchemaKey("com.urbanairship.connect", "CLOSE", "jsonschema", "1-0-0").toSchemaUri,
    "CUSTOM"                        -> SchemaKey("com.urbanairship.connect", "CUSTOM", "jsonschema", "1-0-0").toSchemaUri,
    "FIRST_OPEN"                    -> SchemaKey("com.urbanairship.connect", "FIRST_OPEN", "jsonschema", "1-0-0").toSchemaUri,
    "IN_APP_MESSAGE_DISPLAY"        -> SchemaKey("com.urbanairship.connect", "IN_APP_MESSAGE_DISPLAY", "jsonschema", "1-0-0").toSchemaUri,
    "IN_APP_MESSAGE_EXPIRATION"     -> SchemaKey("com.urbanairship.connect", "IN_APP_MESSAGE_EXPIRATION", "jsonschema", "1-0-0").toSchemaUri,
    "IN_APP_MESSAGE_RESOLUTION"     -> SchemaKey("com.urbanairship.connect", "IN_APP_MESSAGE_RESOLUTION", "jsonschema", "1-0-0").toSchemaUri,
    "LOCATION"                      -> SchemaKey("com.urbanairship.connect", "LOCATION", "jsonschema", "1-0-0").toSchemaUri,
    "OPEN"                          -> SchemaKey("com.urbanairship.connect", "OPEN", "jsonschema", "1-0-0").toSchemaUri,
    "PUSH_BODY"                     -> SchemaKey("com.urbanairship.connect", "PUSH_BODY", "jsonschema", "1-0-0").toSchemaUri,
    "REGION"                        -> SchemaKey("com.urbanairship.connect", "REGION", "jsonschema", "1-0-0").toSchemaUri,
    "RICH_DELETE"                   -> SchemaKey("com.urbanairship.connect", "RICH_DELETE", "jsonschema", "1-0-0").toSchemaUri,
    "RICH_DELIVERY"                 -> SchemaKey("com.urbanairship.connect", "RICH_DELIVERY", "jsonschema", "1-0-0").toSchemaUri,
    "RICH_HEAD"                     -> SchemaKey("com.urbanairship.connect", "RICH_HEAD", "jsonschema", "1-0-0").toSchemaUri,
    "SEND"                          -> SchemaKey("com.urbanairship.connect", "SEND", "jsonschema", "1-0-0").toSchemaUri,
    "TAG_CHANGE"                    -> SchemaKey("com.urbanairship.connect", "TAG_CHANGE", "jsonschema", "1-0-0").toSchemaUri,
    "UNINSTALL"                     -> SchemaKey("com.urbanairship.connect", "UNINSTALL", "jsonschema", "1-0-0").toSchemaUri
  )

  /**
   * Converts payload into a single validated event
   * Expects a valid json, returns failure if one is not present
   *
   * @param body_json json payload as a string
   * @param payload other payload details
   * @return a validated event - a success will contain the corresponding RawEvent, failures will
   *         contain a reason for failure
   */
  private def payloadBodyToEvent(body_json: String, payload: CollectorPayload): Validated[RawEvent] = {

    def toTtmFormat(jsonTimestamp: String) = {
      "%d".format(new DateTime(jsonTimestamp).getMillis)
    }

    try {

      val parsed = parse(body_json)
      val eventType = (parsed \ "type").extractOpt[String]

      val trueTimestamp = (parsed \ "occurred").extractOpt[String]
      val eid = (parsed \ "id").extractOpt[String]
      val collectorTimestamp = (parsed \ "processed").extractOpt[String]

      lookupSchema(eventType, VendorName, EventSchemaMap) map {
        schema => RawEvent(api = payload.api,
          parameters = toUnstructEventParams(TrackerVersion,
            toMap(payload.querystring) ++ Map("ttm" -> toTtmFormat(trueTimestamp.get), "eid" -> eid.get),
            schema,
            parsed,
            "srv"
          ),
          contentType = payload.contentType,
          source = payload.source,
          context = payload.context.copy(timestamp = Some(new DateTime(collectorTimestamp.get, DateTimeZone.UTC)))
        )
      }

    } catch {
      case e: JsonParseException => {
        val exception = JU.stripInstanceEtc(e.toString).orNull
        s"$VendorName event failed to parse into JSON: [$exception]".failureNel
      }
    }

  }

  /**
   * Converts a CollectorPayload instance into raw events.
   * A UrbanAirship connect API payload only contains a single event.
   * We expect the name parameter to match the supported events, else
   * we have an unsupported event type.
   *
   * @param payload The CollectorPayload containing one or more
   *        raw events as collected by a Snowplow collector
   * @param resolver (implicit) The Iglu resolver used for
   *        schema lookup and validation. Not used
   * @return a Validation boxing either a NEL of RawEvents on
   *         Success, or a NEL of Failure Strings
   */
  def toRawEvents(payload: CollectorPayload)(implicit resolver: Resolver): ValidatedRawEvents =
    (payload.body, payload.contentType) match {
      case (None, _) => s"Request body is empty: no ${VendorName} event to process".failureNel
      case (_, Some(ct)) => s"Content type of ${ct} provided, expected None for ${VendorName}".failureNel
      case (Some(body), _) => {
        val event = payloadBodyToEvent(body, payload)
        rawEventsListProcessor(List(event))
      }
    }

}
