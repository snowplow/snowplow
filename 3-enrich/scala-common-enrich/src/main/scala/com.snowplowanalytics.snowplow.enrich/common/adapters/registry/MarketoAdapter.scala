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
package com.snowplowanalytics
package snowplow
package enrich
package common
package adapters
package registry

// Scalaz
import scalaz.Scalaz._

// json4s
import org.json4s._
import org.json4s.jackson.JsonMethods._

// Iglu
import com.snowplowanalytics.iglu.client.{Resolver, SchemaKey}

// Joda Time
import org.joda.time.DateTimeZone
import org.joda.time.format.DateTimeFormat

// This project
import com.snowplowanalytics.snowplow.enrich.common.loaders.CollectorPayload
import com.snowplowanalytics.snowplow.enrich.common.utils.{JsonUtils => JU}

import scala.util.{Failure, Success, Try}

/**
 * Transforms a collector payload which conforms to
 * a known version of the Marketo webhook
 * into raw events.
 */
object MarketoAdapter extends Adapter {

  // Vendor name for Failure Message
  private val VendorName = "Marketo"

  // Expected content type for a request body
  private val ContentType = "application/json"

  // Tracker version for an Marketo webhook
  private val TrackerVersion = "com.marketo-v1"

  // Schemas for reverse-engineering a Snowplow unstructured event
  private val EventSchemaMap = Map(
    "event" -> SchemaKey("com.marketo", "event", "jsonschema", "1-0-0").toSchemaUri
  )

  // Datetime format used by Marketo
  private val MarketoDateTimeFormat = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").withZone(DateTimeZone.UTC)

  /**
   * Returns a validated JSON payload event
   * Converts all date-time values to a valid format
   * The payload will be validated against marketo "event" schema
   *
   * @param json The JSON payload sent by Marketo
   * @param payload Rest of the payload details
   * @return a validated JSON payload on
   *         Success, or a NEL
   */
  private def payloadBodyToEvent(json: String, payload: CollectorPayload): Validated[RawEvent] =
    for {
      parsed <- Try(parse(json)) match {
        case Success(p) => p.successNel
        case Failure(e) => s"$VendorName event failed to parse into JSON: [${e.getMessage}]".failureNel
      }

      parsedConverted = parsed.transformField {
        case ("acquisition_date", JString(value)) =>
          ("acquisition_date", JString(JU.toJsonSchemaDateTime(value, MarketoDateTimeFormat)))
        case ("created_at", JString(value)) =>
          ("created_at", JString(JU.toJsonSchemaDateTime(value, MarketoDateTimeFormat)))
        case ("email_suspended_at", JString(value)) =>
          ("email_suspended_at", JString(JU.toJsonSchemaDateTime(value, MarketoDateTimeFormat)))
        case ("last_referred_enrollment", JString(value)) =>
          ("last_referred_enrollment", JString(JU.toJsonSchemaDateTime(value, MarketoDateTimeFormat)))
        case ("last_referred_visit", JString(value)) =>
          ("last_referred_visit", JString(JU.toJsonSchemaDateTime(value, MarketoDateTimeFormat)))
        case ("updated_at", JString(value)) =>
          ("updated_at", JString(JU.toJsonSchemaDateTime(value, MarketoDateTimeFormat)))
        case ("datetime", JString(value)) =>
          ("datetime", JString(JU.toJsonSchemaDateTime(value, MarketoDateTimeFormat)))
      }
      // The payload doesn't contain a "type" field so we're constraining the eventType to be of type "event"
      eventType = Some("event")
      schema <- lookupSchema(eventType, VendorName, EventSchemaMap)
      params = toUnstructEventParams(TrackerVersion, toMap(payload.querystring), schema, parsedConverted, "srv")
      rawEvent = RawEvent(api = payload.api,
                          parameters  = params,
                          contentType = payload.contentType,
                          source      = payload.source,
                          context     = payload.context)
    } yield rawEvent

  /**
   * Converts a CollectorPayload instance into raw events.
   * Marketo event contains no "type" field and since there's only 1 schema the function lookupschema takes the eventType parameter as "event".
   * We expect the type parameter to match the supported events, else
   * we have an unsupported event type.
   *
   * @param payload The CollectorPayload containing one or more
   *        raw events as collected by a Snowplow collector
   * @param resolver (implicit) The Iglu resolver used for
   *        schema lookup and validation. Not used
   * @return a Validation boxing either a NEL of RawEvents on
   *         Success, or a NEL of Failure Strings
   */
  override def toRawEvents(payload: CollectorPayload)(implicit resolver: Resolver): ValidatedRawEvents =
    (payload.body, payload.contentType) match {
      case (None, _) => s"Request body is empty: no $VendorName event to process".failureNel
      case (Some(body), _) => {
        val event = payloadBodyToEvent(body, payload)
        rawEventsListProcessor(List(event))
      }
    }
}
