/*
 * Copyright (c) 2014-2019 Snowplow Analytics Ltd. All rights reserved.
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

// Jackson
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.core.JsonParseException

// Scalaz
import scalaz._
import Scalaz._

// json4s
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import org.json4s.scalaz.JsonScalaz._

// Iglu
import iglu.client.{Resolver, SchemaKey}
import iglu.client.validation.ValidatableJsonMethods._

// Joda Time
import org.joda.time.DateTime

// This project
import loaders.CollectorPayload
import utils.{JsonUtils => JU}

/**
 * Transforms a collector payload which conforms to
 * a known version of the HubSpot webhook subscription
 * into raw events.
 */
object HubSpotAdapter extends Adapter {

  // Vendor name for Failure Message
  private val VendorName = "HubSpot"

  // Tracker version for a HubSpot webhook
  private val TrackerVersion = "com.hubspot-v1"

  // Expected content type for a request body
  private val ContentType = "application/json"

  // Event-Schema Map for reverse-engineering a Snowplow unstructured event
  private val EventSchemaMap = Map(
    "contact.creation"       -> SchemaKey("com.hubspot", "contact_creation", "jsonschema", "1-0-0").toSchemaUri,
    "contact.deletion"       -> SchemaKey("com.hubspot", "contact_deletion", "jsonschema", "1-0-0").toSchemaUri,
    "contact.propertyChange" -> SchemaKey("com.hubspot", "contact_change", "jsonschema", "1-0-0").toSchemaUri,
    "company.creation"       -> SchemaKey("com.hubspot", "company_creation", "jsonschema", "1-0-0").toSchemaUri,
    "company.deletion"       -> SchemaKey("com.hubspot", "company_deletion", "jsonschema", "1-0-0").toSchemaUri,
    "company.propertyChange" -> SchemaKey("com.hubspot", "company_change", "jsonschema", "1-0-0").toSchemaUri,
    "deal.creation"          -> SchemaKey("com.hubspot", "deal_creation", "jsonschema", "1-0-0").toSchemaUri,
    "deal.deletion"          -> SchemaKey("com.hubspot", "deal_deletion", "jsonschema", "1-0-0").toSchemaUri,
    "deal.propertyChange"    -> SchemaKey("com.hubspot", "deal_change", "jsonschema", "1-0-0").toSchemaUri
  )

  /**
   * Converts a CollectorPayload instance into raw events.
   * A HubSpot Tracking payload can contain many events in one.
   * We expect the type parameter to be 1 of 9 options otherwise
   * we have an unsupported event type.
   *
   * @param payload The CollectorPaylod containing one or more
   *        raw events as collected by a Snowplow collector
   * @param resolver (implicit) The Iglu resolver used for
   *        schema lookup and validation. Not used
   * @return a Validation boxing either a NEL of RawEvents on
   *         Success, or a NEL of Failure Strings
   */
  def toRawEvents(payload: CollectorPayload)(implicit resolver: Resolver): ValidatedRawEvents =
    (payload.body, payload.contentType) match {
      case (None, _) => s"Request body is empty: no ${VendorName} events to process".failNel
      case (_, None) =>
        s"Request body provided but content type empty, expected ${ContentType} for ${VendorName}".failNel
      case (_, Some(ct)) if ct != ContentType =>
        s"Content type of ${ct} provided, expected ${ContentType} for ${VendorName}".failNel
      case (Some(body), _) => {

        payloadBodyToEvents(body) match {
          case Failure(str) => str.failNel
          case Success(list) => {

            // Create our list of Validated RawEvents
            val rawEventsList: List[Validated[RawEvent]] =
              for {
                (event, index) <- list.zipWithIndex
              } yield {

                val eventType: Option[String] = (event \ "subscriptionType").extractOpt[String]
                for {
                  schema <- lookupSchema(eventType, VendorName, index, EventSchemaMap)
                } yield {

                  val formattedEvent = reformatParameters(event)
                  val qsParams       = toMap(payload.querystring)
                  RawEvent(
                    api         = payload.api,
                    parameters  = toUnstructEventParams(TrackerVersion, qsParams, schema, formattedEvent, "srv"),
                    contentType = payload.contentType,
                    source      = payload.source,
                    context     = payload.context
                  )
                }
              }

            // Processes the List for Failures and Successes and returns ValidatedRawEvents
            rawEventsListProcessor(rawEventsList)
          }
        }
      }
    }

  /**
   * Returns a list of JValue events from the
   * HubSpot payload
   *
   * @param body The payload body from the HubSpot
   *        event
   * @return either a Successful List of JValue JSONs
   *         or a Failure String
   */
  private[registry] def payloadBodyToEvents(body: String): Validation[String, List[JValue]] =
    try {
      val parsed = parse(body)
      parsed match {
        case JArray(list) => list.success
        case _            => s"Could not resolve ${VendorName} payload into a JSON array of events".fail
      }
    } catch {
      case e: JsonParseException => {
        val exception = JU.stripInstanceEtc(e.toString).orNull
        s"${VendorName} payload failed to parse into JSON: [${exception}]".fail
      }
    }

  /**
   * Returns an updated HubSpot event JSON where
   * the "subscriptionType" field is removed
   * and "occurredAt" fields' values have been converted
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

    json removeField {
      case ("subscriptionType", JString(s)) => true
      case _                                => false
    } transformField {
      case ("occurredAt", JInt(value)) => ("occurredAt", toStringField(value.toLong))
    }
  }
}
