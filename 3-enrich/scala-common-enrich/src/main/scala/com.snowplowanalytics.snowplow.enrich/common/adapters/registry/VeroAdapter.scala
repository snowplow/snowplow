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
//import com.snowplowanalytics.snowplow.enrich.common.utils.{JsonUtils => JU}

import scala.util.{Failure, Success, Try}

/**
 * Transforms a collector payload which conforms to
 * a known version of the Vero webhook
 * into raw events.
 */
object VeroAdapter extends Adapter {

  // Vendor name for Failure Message
  private val VendorName = "Vero"

  // Expected content type for a request body
  private val ContentType = "application/json"

  // Tracker version for an Vero webhook
  private val TrackerVersion = "com.getvero-v1"

  // Schemas for reverse-engineering a Snowplow unstructured event
  private val EventSchemaMap = Map(
    "bounced"      -> SchemaKey("com.getvero", "bounced", "jsonschema", "1-0-0").toSchemaUri,
    "clicked"      -> SchemaKey("com.getvero", "clicked", "jsonschema", "1-0-0").toSchemaUri,
    "delivered"    -> SchemaKey("com.getvero", "delivered", "jsonschema", "1-0-0").toSchemaUri,
    "opened"       -> SchemaKey("com.getvero", "opened", "jsonschema", "1-0-0").toSchemaUri,
    "sent"         -> SchemaKey("com.getvero", "sent", "jsonschema", "1-0-0").toSchemaUri,
    "unsubscribed" -> SchemaKey("com.getvero", "unsubscribed", "jsonschema", "1-0-0").toSchemaUri,
    "user_created" -> SchemaKey("com.getvero", "created", "jsonschema", "1-0-0").toSchemaUri,
    "user_updated" -> SchemaKey("com.getvero", "updated", "jsonschema", "1-0-0").toSchemaUri
  )

  /**
   * Converts a payload into a single validated event
   * Expects a valid json returns failure if one is not present
   *
   * @param json Payload body that is sent by Vero
   * @param payload The details of the payload
   * @return a Validation boxing either a NEL of RawEvents on
   *         Success, or a NEL of Failure Strings
   */
  private def payloadBodyToEvent(json: String, payload: CollectorPayload): Validated[RawEvent] =
    for {
      parsed <- Try(parse(json)) match {
        case Success(p) => p.successNel
        case Failure(e) => s"$VendorName event failed to parse into JSON: [${e.getMessage}]".failureNel
      }
      eventType        = (parsed \ "type").extract[String]
      formattedEvent   = cleanupJsonEventValues(parsed, ("type", eventType).some, s"${eventType}_at")
      reformattedEvent = reformatParameters(formattedEvent)
      schema <- lookupSchema(eventType.some, VendorName, EventSchemaMap)
      params = toUnstructEventParams(TrackerVersion, toMap(payload.querystring), schema, reformattedEvent, "srv")
      rawEvent = RawEvent(api = payload.api,
                          parameters  = params,
                          contentType = payload.contentType,
                          source      = payload.source,
                          context     = payload.context)
    } yield rawEvent

  /**
   * Converts a CollectorPayload instance into raw events.
   * A Vero API payload only contains a single event.
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

  /**
   * Returns an updated Vero event JSON where
   * the "_tag" field is renamed to "tag"
   * and "triggered_at" fields' values have been converted
   *
   * @param json The event JSON which we need to
   *        update values for
   * @return the updated JSON with updated fields and values
   */
  def reformatParameters(json: JValue): JValue = {

    def toStringField(value: Long): JString = {
      val dt: DateTime = new DateTime(value)
      JString(JsonSchemaDateTimeFormat.print(dt))
    }

    json transformField {
      case ("_tags", JObject(v))         => ("tags", JObject(v))
      case ("triggered_at", JInt(value)) => ("triggered_at", toStringField(value.toLong * 1000))
    }
  }
}
