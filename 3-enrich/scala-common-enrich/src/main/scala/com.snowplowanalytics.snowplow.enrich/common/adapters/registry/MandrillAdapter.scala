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

// Java
import java.net.URI
import org.apache.http.client.utils.URLEncodedUtils

// Joda-Time
import org.joda.time.{DateTime, DateTimeZone}
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}

// Jackson
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

// Iglu
import iglu.client.{
  SchemaKey,
  Resolver
}

// This project
import loaders.CollectorPayload
import utils.{JsonUtils => JU}

/**
 * Transforms a collector payload which conforms to
 * a known version of the Mandrill Tracking webhook
 * into raw events.
 */
object MandrillAdapter extends Adapter {

  // Vendor name for Failure Message
  private val VendorName = "Mandrill"

  // Tracker version for an Mandrill Tracking webhook
  private val TrackerVersion = "com.mandrill-v1"

  // Expected content type for a request body
  private val ContentType = "application/x-www-form-urlencoded"

  // Schemas for reverse-engineering a Snowplow unstructured event
  private val EventSchemaMap = Map (
    "hard_bounce" -> SchemaKey("com.mandrill", "message_bounced", "jsonschema", "1-0-0").toSchemaUri,
    "click"       -> SchemaKey("com.mandrill", "message_clicked", "jsonschema", "1-0-0").toSchemaUri,
    "deferral"    -> SchemaKey("com.mandrill", "message_delayed", "jsonschema", "1-0-0").toSchemaUri,
    "spam"        -> SchemaKey("com.mandrill", "message_marked_as_spam", "jsonschema", "1-0-0").toSchemaUri,
    "open"        -> SchemaKey("com.mandrill", "message_opened", "jsonschema", "1-0-0").toSchemaUri,
    "reject"      -> SchemaKey("com.mandrill", "message_rejected", "jsonschema", "1-0-0").toSchemaUri,
    "send"        -> SchemaKey("com.mandrill", "message_sent", "jsonschema", "1-0-0").toSchemaUri,
    "soft_bounce" -> SchemaKey("com.mandrill", "message_soft_bounced", "jsonschema", "1-0-0").toSchemaUri,
    "unsub"       -> SchemaKey("com.mandrill", "recipient_unsubscribed", "jsonschema", "1-0-0").toSchemaUri
  )

  // Datetime format we need for all 'ts' fields within a Mandrill Event
  private val JsonSchemaDateTimeFormat = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").withZone(DateTimeZone.UTC)

  /**
   * Converts a CollectorPayload instance into raw events.
   *
   * A Mandrill Tracking payload contains many events in 
   * the body of the payload, stored within a HTTP encoded
   * string.
   * We expect the event parameter of these events to be 
   * 1 of 9 options otherwise we have an unsupported event 
   * type.
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
      case (None, _)                          => s"Request body is empty: no ${VendorName} events to process".failNel
      case (_, None)                          => s"Request body provided but content type empty, expected ${ContentType} for ${VendorName}".failNel
      case (_, Some(ct)) if ct != ContentType => s"Content type of ${ct} provided, expected ${ContentType} for ${VendorName}".failNel
      case (Some(body),_)                     => {

        payloadBodyToEvents(body) match {
          case Failure(str)  => str.failNel
          case Success(list) => {

            // Create our list of Validated RawEvents
            val rawEventsList: List[Validated[RawEvent]] = 
              for { 
                (event, index) <- list.zipWithIndex
              } yield {

                val eventOpt: Option[String] = (event \ "event").extractOpt[String]
                for {
                  schema <- lookupSchema(eventOpt, VendorName, index, EventSchemaMap)
                } yield {
                  
                  val formattedEvent = reformatParameters(event, eventOpt)
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
   * Returns a list of events from the payload 
   * body of a Mandrill Event.  Each event will
   * be formatted as an individual JSON of type
   * JValue.
   * 
   * NOTE:
   * The payload.body string must adhere to UTF-8
   * encoding standards.
   *
   * @param rawEventString The encoded string 
   *        from the Mandrill payload body
   * @return a list of single events formatted as 
   *         json4s JValue JSONs or a Failure String
   */
  private[registry] def payloadBodyToEvents(rawEventString: String): Validation[String,List[JValue]] = {

    val bodyMap = toMap(URLEncodedUtils.parse(URI.create("http://localhost/?" + rawEventString), "UTF-8").toList)

    bodyMap match {
      case map if map.size != 1 => s"Mapped ${VendorName} body has invalid count of keys: ${map.size}".fail
      case map                  => {
        map.get("mandrill_events") match {
          case None       => s"Mapped ${VendorName} body does not have 'mandrill_events' as a key".fail
          case Some("")   => s"${VendorName} events string is empty: nothing to process".fail
          case Some(dStr) => {
            try {
              val parsed = parse(dStr)
              parsed match {
                case JArray(list) => list.success
                case _            => s"Could not resolve ${VendorName} payload into a JSON array of events".fail
              }
            } catch {
              case e: JsonParseException => {
                val exception = JU.stripInstanceEtc(e.toString)
                s"${VendorName} events string failed to parse into JSON: [$exception]".fail
              }
            }
          }
        }
      }
    }
  }

  /**
   * Returns an updated Mandrill Event JSON where 
   * all of the timestamp fields ("ts":_) have been 
   * changed to a valid JsonSchema date-time format
   * and the "event":_type field has been removed
   *
   * @param json The event JSON which we need to
   *        update values for
   * @param eventOpt The event type as an Option[String]
   *        which we are now going to remove from
   *        the event JSON
   * @return the updated JSON with valid date-time
   *         values in the 'ts' fields
   */
  private[registry] def reformatParameters(json: JValue, eventOpt: Option[String]): JValue = {

    val j1 = json transformField {
      case ("ts", JInt(x)) => {
        try {
          val dt: DateTime = new DateTime(x.longValue() * 1000)
          ("ts", JString(JsonSchemaDateTimeFormat.print(dt)))
        } catch {
          case _ : Throwable => ("ts", JInt(x))
        }
      }
    }

    eventOpt match {
      case Some(eventType) => j1 removeField { _ == JField("event", eventType) }
      case None            => j1
    }
  }
}
