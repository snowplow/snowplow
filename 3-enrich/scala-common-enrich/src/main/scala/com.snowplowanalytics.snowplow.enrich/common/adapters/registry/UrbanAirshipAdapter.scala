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

import cats.data.{NonEmptyList, ValidatedNel}
import cats.syntax.apply._
import cats.syntax.either._
import cats.syntax.validated._
import com.snowplowanalytics.iglu.client.{Resolver, SchemaKey}
import io.circe.parser._
import org.joda.time.{DateTime, DateTimeZone}

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
  ): ValidatedNel[String, RawEvent] = {
    def toTtmFormat(jsonTimestamp: String) =
      "%d".format(new DateTime(jsonTimestamp).getMillis)

    parse(bodyJson) match {
      case Right(json) =>
        val cursor = json.hcursor
        val eventType = cursor.get[String]("type").toOption
        val trueTs = cursor.get[String]("occurred").leftMap(_.getMessage).toValidatedNel
        val eid = cursor.get[String]("id").leftMap(_.getMessage).toValidatedNel
        val collectorTs = cursor.get[String]("processed").leftMap(_.getMessage).toValidatedNel
        (
          trueTs,
          eid,
          collectorTs,
          lookupSchema(eventType, VendorName, EventSchemaMap).toValidatedNel
        ).mapN { (tts, id, cts, schema) =>
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
      case Left(e) => s"$VendorName event failed to parse into JSON: [${e.getMessage}]".invalidNel
    }
  }

  /**
   * Converts a CollectorPayload instance into raw events. A UrbanAirship connect API payload only
   * contains a single event. We expect the name parameter to match the supported events, else
   * we have an unsupported event type.
   * @param payload The CollectorPayload containing one or more raw events
   * @param resolver (implicit) The Iglu resolver used for schema lookup and validation. Not used
   * @return a Validation boxing either a NEL of RawEvents on Success, or a NEL of Failure Strings
   */
  def toRawEvents(payload: CollectorPayload)(
    implicit resolver: Resolver
  ): ValidatedNel[String, NonEmptyList[RawEvent]] =
    (payload.body, payload.contentType) match {
      case (None, _) => s"Request body is empty: no $VendorName event to process".invalidNel
      case (_, Some(ct)) =>
        s"Content type of $ct provided, expected None for $VendorName".invalidNel
      case (Some(body), _) =>
        val event = payloadBodyToEvent(body, payload)
        rawEventsListProcessor(List(event))
    }

}
