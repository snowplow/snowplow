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

import cats.syntax.either._
import com.snowplowanalytics.iglu.client.{Resolver, SchemaKey}
import io.circe.parser._
import org.joda.time.{DateTime, DateTimeZone}
import scalaz.Scalaz._

import loaders.CollectorPayload

/**
 * Transforms a collector payload which conforms to a known version of the UrbanAirship Connect API
 * into raw events.
 */
object UrbanAirshipAdapter extends Adapter {
  // Vendor name for Failure Message
  private val VendorName = "UrbanAirship"

  // Tracker version for an UrbanAirship Connect API
  private val TrackerVersion = "com.urbanairship.connect-v1"

  // Schemas for reverse-engineering a Snowplow unstructured event
  private val EventSchemaMap = Map(
    "CLOSE" -> SchemaKey("com.urbanairship.connect", "CLOSE", "jsonschema", "1-0-0").toSchemaUri,
    "CUSTOM" -> SchemaKey("com.urbanairship.connect", "CUSTOM", "jsonschema", "1-0-0").toSchemaUri,
    "FIRST_OPEN" -> SchemaKey("com.urbanairship.connect", "FIRST_OPEN", "jsonschema", "1-0-0").toSchemaUri,
    "IN_APP_MESSAGE_DISPLAY" -> SchemaKey(
      "com.urbanairship.connect",
      "IN_APP_MESSAGE_DISPLAY",
      "jsonschema",
      "1-0-0"
    ).toSchemaUri,
    "IN_APP_MESSAGE_EXPIRATION" -> SchemaKey(
      "com.urbanairship.connect",
      "IN_APP_MESSAGE_EXPIRATION",
      "jsonschema",
      "1-0-0"
    ).toSchemaUri,
    "IN_APP_MESSAGE_RESOLUTION" -> SchemaKey(
      "com.urbanairship.connect",
      "IN_APP_MESSAGE_RESOLUTION",
      "jsonschema",
      "1-0-0"
    ).toSchemaUri,
    "LOCATION" -> SchemaKey("com.urbanairship.connect", "LOCATION", "jsonschema", "1-0-0").toSchemaUri,
    "OPEN" -> SchemaKey("com.urbanairship.connect", "OPEN", "jsonschema", "1-0-0").toSchemaUri,
    "PUSH_BODY" -> SchemaKey("com.urbanairship.connect", "PUSH_BODY", "jsonschema", "1-0-0").toSchemaUri,
    "REGION" -> SchemaKey("com.urbanairship.connect", "REGION", "jsonschema", "1-0-0").toSchemaUri,
    "RICH_DELETE" -> SchemaKey("com.urbanairship.connect", "RICH_DELETE", "jsonschema", "1-0-0").toSchemaUri,
    "RICH_DELIVERY" -> SchemaKey("com.urbanairship.connect", "RICH_DELIVERY", "jsonschema", "1-0-0").toSchemaUri,
    "RICH_HEAD" -> SchemaKey("com.urbanairship.connect", "RICH_HEAD", "jsonschema", "1-0-0").toSchemaUri,
    "SEND" -> SchemaKey("com.urbanairship.connect", "SEND", "jsonschema", "1-0-0").toSchemaUri,
    "TAG_CHANGE" -> SchemaKey("com.urbanairship.connect", "TAG_CHANGE", "jsonschema", "1-0-0").toSchemaUri,
    "UNINSTALL" -> SchemaKey("com.urbanairship.connect", "UNINSTALL", "jsonschema", "1-0-0").toSchemaUri
  )

  /**
   * Converts payload into a single validated event. Expects a valid json, returns failure if one is
   * not present.
   * @param body_json json payload as a string
   * @param payload other payload details
   * @return a validated event - a success is the RawEvent, failures will contain the reasons
   */
  private def payloadBodyToEvent(
    bodyJson: String,
    payload: CollectorPayload
  ): Validated[RawEvent] = {
    def toTtmFormat(jsonTimestamp: String) =
      "%d".format(new DateTime(jsonTimestamp).getMillis)

    parse(bodyJson) match {
      case Right(json) =>
        val cursor = json.hcursor
        val eventType = cursor.get[String]("type").toOption
        val trueTs = cursor.get[String]("occurred").toOption
        val eid = cursor.get[String]("id").toOption
        val collectorTs = cursor.get[String]("processed").toOption
        (trueTs |@| eid |@| collectorTs) { (tts, id, cts) =>
          lookupSchema(eventType, VendorName, EventSchemaMap).map { schema =>
            RawEvent(
              api = payload.api,
              parameters = toUnstructEventParams(
                TrackerVersion,
                toMap(payload.querystring) ++ Map("ttm" -> toTtmFormat(tts), "eid" -> id),
                schema,
                json,
                "srv"
              ),
              contentType = payload.contentType,
              source = payload.source,
              context = payload.context.copy(timestamp = Some(new DateTime(cts, DateTimeZone.UTC)))
            )
          }
        }.getOrElse(s"$VendorName malformed 'occurred', 'id' or 'processed' fields".failNel)
      case Left(e) => s"$VendorName event failed to parse into JSON: [${e.getMessage}]".failNel
    }

    /**try {
      val parsed = parse(body_json)
      val eventType = (parsed \ "type").extractOpt[String]
      val trueTimestamp = (parsed \ "occurred").extractOpt[String]
      val eid = (parsed \ "id").extractOpt[String]
      val collectorTimestamp = (parsed \ "processed").extractOpt[String]

      lookupSchema(eventType, VendorName, EventSchemaMap).map { schema =>
        RawEvent(
          api = payload.api,
          parameters = toUnstructEventParams(
            TrackerVersion,
            toMap(payload.querystring) ++ Map(
              "ttm" -> toTtmFormat(trueTimestamp.get),
              "eid" -> eid.get),
            schema,
            parsed,
            "srv"),
          contentType = payload.contentType,
          source = payload.source,
          context = payload.context.copy(
            timestamp = Some(new DateTime(collectorTimestamp.get, DateTimeZone.UTC)))
        )
      }
    } catch {
      case e: JsonParseException => {
        val exception = JU.stripInstanceEtc(e.toString).orNull
        s"$VendorName event failed to parse into JSON: [$exception]".failNel
      }
    }**/

  }

  /**
   * Converts a CollectorPayload instance into raw events. A UrbanAirship connect API payload only
   * contains a single event. We expect the name parameter to match the supported events, else
   * we have an unsupported event type.
   * @param payload The CollectorPayload containing one or more raw events
   * @param resolver (implicit) The Iglu resolver used for schema lookup and validation. Not used
   * @return a Validation boxing either a NEL of RawEvents on Success, or a NEL of Failure Strings
   */
  def toRawEvents(payload: CollectorPayload)(implicit resolver: Resolver): ValidatedRawEvents =
    (payload.body, payload.contentType) match {
      case (None, _) => s"Request body is empty: no $VendorName event to process".failNel
      case (_, Some(ct)) =>
        s"Content type of $ct provided, expected None for $VendorName".failNel
      case (Some(body), _) =>
        val event = payloadBodyToEvent(body, payload)
        rawEventsListProcessor(List(event))
    }

}
