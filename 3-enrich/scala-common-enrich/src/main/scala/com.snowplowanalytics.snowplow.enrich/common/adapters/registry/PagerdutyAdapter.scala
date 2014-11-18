/*
 * Copyright (c) 2014 Snowplow Analytics Ltd. All rights reserved.
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

// Iglu
import iglu.client.{
  SchemaKey,
  Resolver
}
import iglu.client.validation.ValidatableJsonMethods._

// Jackson
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.core.JsonParseException

// Scala
import scala.collection.JavaConversions._

// Scalaz
import scalaz._
import Scalaz._

// json4s
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import org.json4s.scalaz.JsonScalaz._

// This project
import loaders.CollectorPayload
import utils.{JsonUtils => JU}

/**
 * Transforms a collector payload which conforms to
 * a known version of the PagerDuty Tracking webhook
 * into raw events.
 */
object PagerdutyAdapter extends Adapter {

  // Vendor name for Failure Message
  private val VendorName = "PagerDuty"

  // Tracker version for a PagerDuty webhook
  private val TrackerVersion = "com.pagerduty-v1"

  // Expected content type for a request body
  private val ContentType = "application/json"

  // Event-Schema Map for reverse-engineering a Snowplow unstructured event
  private val Incident = SchemaKey("com.pagerduty", "incident", "jsonschema", "1-0-0").toSchemaUri
  private val EventSchemaMap = Map(
    "incident.trigger"       -> Incident,
    "incident.acknowledge"   -> Incident,
    "incident.unacknowledge" -> Incident,
    "incident.resolve"       -> Incident,
    "incident.assign"        -> Incident,
    "incident.escalate"      -> Incident,
    "incident.delegate"      -> Incident
  )
  
 /**
   * Converts a CollectorPayload instance into raw events.
   * A PagerDuty Tracking payload can contain many events in one.
   * We expect the type parameter to be 1 of 7 options otherwise
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
      case (None, _)                          => s"Request body is empty: no PagerDuty events to process".failNel
      case (_, None)                          => s"Request body provided but content type empty, expected ${ContentType} for PagerDuty".failNel
      case (_, Some(ct)) if ct != ContentType => s"Content type of ${ct} provided, expected ${ContentType} for PagerDuty".failNel
      case (Some(body),_)                     => {

        payloadBodyToEventList(body) match {
          case Failure(str)  => str.failNel
          case Success(list) => {

            // Create our list of Validated RawEvents
            val rawEventsList: List[Validated[RawEvent]] = 
              for { 
                (event, index) <- list.zipWithIndex
              } yield {

                val eventOpt: Option[String] = (event \ "type").extractOpt[String]
                for {
                  schema <- lookupSchema(eventOpt, VendorName, index, EventSchemaMap)
                } yield {

                  val formattedEvent = reformatParameters(event)
                  val qsParams = toMap(payload.querystring)
                  RawEvent(
                    api          = payload.api,
                    parameters   = toUnstructEventParams(TrackerVersion, qsParams, schema, formattedEvent, "srv"),
                    contentType  = payload.contentType,
                    source       = payload.source,
                    context      = payload.context
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
   * PagerDuty payload
   *
   * @param body The payload body from the PagerDuty
   *        event
   * @return either a Successful List of JValue JSONs
   *         or a Failure String 
   */
  private[registry] def payloadBodyToEventList(body: String): Validation[String,List[JValue]] =
    try {
      val parsed = parse(body)
      (parsed \ "messages") match {
        case JArray(list) => list.success
        case JNothing     => s"PagerDuty payload does not contain the needed 'messages' key".fail
        case _            => s"Could not resolve PagerDuty payload into a JSON array of events".fail
      }
    } catch {
      case e: JsonParseException => {
        val exception = JU.stripInstanceEtc(e.toString)
        s"PagerDuty payload failed to parse into JSON: [$exception]".fail
      }
    }

  /**
   * Returns an updated event JSON where 
   * all of the fields with a null string
   * have been changed to a null value
   *
   * @param json The event JSON which we need to
   *        update values within
   * @return the updated JSON with valid date-time
   *         values in the 'ts' fields
   */
  private[registry] def reformatParameters(json: JValue): JValue =
    json transformField {
      case (key, JString("null")) => (key, JNull)
      case ("type", JString(value)) if value.startsWith("incident.") => ("type", JString(value.replace("incident.", "")))
    }
}
