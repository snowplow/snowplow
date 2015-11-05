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

// Scalaz
import com.snowplowanalytics.snowplow.enrich.common.adapters.registry.SendgridAdapter._

import scalaz.Scalaz._
import scalaz._

// json4s
import org.json4s._
import org.json4s.jackson.JsonMethods._

// Iglu
import com.snowplowanalytics.iglu.client.{Resolver, SchemaKey}

// This project
import com.snowplowanalytics.snowplow.enrich.common.loaders.CollectorPayload
import com.snowplowanalytics.snowplow.enrich.common.utils.{JsonUtils => JU}

/**
  * Transforms a collector payload which conforms to
  * a known version of the Sendgrid Tracking webhook
  * into raw events.
  */
object SendgridAdapter extends Adapter {

  // Vendor name for Failure Message
  private val VendorName = "Sendgrid"

  // Expected content type for a request body
  private val ContentType = "application/x-www-form-urlencoded"

  // Tracker version for an Sendgrid Tracking webhook
  private val TrackerVersion = "com.sendgrid-v3"

  // Schemas for reverse-engineering a Snowplow unstructured event
  private val EventSchemaMap = Map(
    "processed" -> SchemaKey("com.sendgrid", "processed", "jsonschema", "1-0-0").toSchemaUri,
    "dropped" -> SchemaKey("com.sendgrid", "dropped", "jsonschema", "1-0-0").toSchemaUri,
    "delivered" -> SchemaKey("com.sendgrid", "delivered", "jsonschema", "1-0-0").toSchemaUri,
    "deferred" -> SchemaKey("com.sendgrid", "deferred", "jsonschema", "1-0-0").toSchemaUri,
    "bounce" -> SchemaKey("com.sendgrid", "bounce", "jsonschema", "1-0-0").toSchemaUri,
    "open" -> SchemaKey("com.sendgrid", "open", "jsonschema", "1-0-0").toSchemaUri,
    "click" -> SchemaKey("com.sendgrid", "click", "jsonschema", "1-0-0").toSchemaUri,
    "spamreport" -> SchemaKey("com.sendgrid", "spamreport", "jsonschema", "1-0-0").toSchemaUri,
    "unsubscribe" -> SchemaKey("com.sendgrid", "unsubscribe", "jsonschema", "1-0-0").toSchemaUri,
    "group_unsubscribe" -> SchemaKey("com.sendgrid", "group_unsubscribe", "jsonschema", "1-0-0").toSchemaUri,
    "group_resubscribe" -> SchemaKey("com.sendgrid", "group_resubscribe", "jsonschema", "1-0-0").toSchemaUri
  )


  def payloadBodyToEvents(body: String, payload: CollectorPayload): List[Validated[RawEvent]] = {

    parse(body).children.map(itm => {

      val eventType = (itm \\ "event").extract[String]
      val queryString = toMap(payload.querystring)

      lookupSchema(eventType.some, VendorName, EventSchemaMap) match {
        case Success(schema) => {
          Success(
            RawEvent(
              api = payload.api,
              parameters = toUnstructEventParams(TrackerVersion,
                queryString,
                schema,
                itm,
                "srv"),
              contentType = payload.contentType,
              source = payload.source,
              context = payload.context
            )
          )
        }
        case _ =>  s"Unsupported event type of $eventType provided".failNel
      }

    })

  }

  /**
    * Converts a CollectorPayload instance into raw events.
    * A Sendgrid Tracking payload only contains a single event.
    * We expect the name parameter to be 1 of 6 options otherwise
    * we have an unsupported event type.
    *
    * @param payload The CollectorPayload containing one or more
    *                raw events as collected by a Snowplow collector
    * @param resolver (implicit) The Iglu resolver used forValidatedRawEvents
    *                 schema lookup and validation. Not used
    * @return a Validation boxing either a NEL of RawEvents on
    *         Success, or a NEL of Failure Strings
    */
  def toRawEvents(payload: CollectorPayload)(implicit resolver: Resolver): ValidatedRawEvents =
    (payload.body, payload.contentType) match {
      case (None, _) => s"Request body is empty: no ${VendorName} event to process".failNel
      case (_, None) => s"Request body provided but content type empty, expected ${ContentType} for ${VendorName}".failNel
      case (_, Some(ct)) if ct != ContentType => s"Content type of ${ct} provided, expected ${ContentType} for ${VendorName}".failNel
      case (Some(body), _) => {
        val events = payloadBodyToEvents(body, payload)
        rawEventsListProcessor(events)
      }
    }
}
