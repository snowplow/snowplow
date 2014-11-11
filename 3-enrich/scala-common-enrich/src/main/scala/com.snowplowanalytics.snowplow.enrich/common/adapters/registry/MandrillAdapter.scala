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

// Java
import java.net.URI
import org.apache.http.client.utils.URLEncodedUtils

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

// Joda-Time
import org.joda.time.{DateTime, DateTimeZone}
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}

// This project
import loaders.CollectorPayload
import utils.{JsonUtils => JU}

/**
 * Transforms a collector payload which conforms to
 * a known version of the Mandrill Tracking webhook
 * into raw events.
 */
object MandrillAdapter extends Adapter {

  // Needed for json4s default extraction formats
  implicit val formats = DefaultFormats

  // Tracker version for an Mandrill Tracking webhook
  private val TrackerVersion = "com.mandrill-v1"

  // Expected content type for a request body
  private val ContentType = "application/x-www-form-urlencoded"

  // Schemas for reverse-engineering a Snowplow unstructured event
  private object SchemaUris {
    val UnstructEvent         = SchemaKey("com.snowplowanalytics.snowplow", "unstruct_event", "jsonschema", "1-0-0").toSchemaUri
    val MessageBounced        = SchemaKey("com.mandrill", "message_bounced", "jsonschema", "1-0-0").toSchemaUri
    val MessageClicked        = SchemaKey("com.mandrill", "message_clicked", "jsonschema", "1-0-0").toSchemaUri
    val MessageDelayed        = SchemaKey("com.mandrill", "message_delayed", "jsonschema", "1-0-0").toSchemaUri
    val MessageMarkedAsSpam   = SchemaKey("com.mandrill", "message_marked_as_spam", "jsonschema", "1-0-0").toSchemaUri
    val MessageOpened         = SchemaKey("com.mandrill", "message_opened", "jsonschema", "1-0-0").toSchemaUri
    val MessageRejected       = SchemaKey("com.mandrill", "message_rejected", "jsonschema", "1-0-0").toSchemaUri
    val MessageSent           = SchemaKey("com.mandrill", "message_sent", "jsonschema", "1-0-0").toSchemaUri
    val MessageSoftBounced    = SchemaKey("com.mandrill", "message_soft_bounced", "jsonschema", "1-0-0").toSchemaUri
    val RecipientUnsubscribed = SchemaKey("com.mandrill", "recipient_unsubscribed", "jsonschema", "1-0-0").toSchemaUri
  }

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
      case (None, _)                          => s"Request body is empty: no Mandrill events to process".failNel
      case (_, None)                          => s"Request body provided but content type empty, expected ${ContentType} for Mandrill".failNel
      case (_, Some(ct)) if ct != ContentType => s"Content type of ${ct} provided, expected ${ContentType} for Mandrill".failNel
      case (Some(body),_)                     => {

        bodyToEventList(body) match {
          case Failure(str)  => str.failNel
          case Success(list) => {

            // Create our list of Validated RawEvents
            val rawEventsList: List[Validation[NonEmptyList[String],RawEvent]] = {
              for { 
                event <- list
              } yield {
                jsonToRawEvent(payload, event)
              }
            }
            
            // Gather all of our Successes and Failures into seperate lists
            val successes: List[RawEvent] = {
              for {
                Success(s) <- rawEventsList 
              } yield s
            }
            val failures: List[String] = {
              for {
                Failure(NonEmptyList(f)) <- rawEventsList 
              } yield f
            }

            // Send out our ValidatedRawEvents (either a Nel of Failures or a Nel of RawEvents)
            // If we have any Failures we will discard everything but these Failures.
            (successes, failures) match {
              case (s :: ss,     Nil) =>  NonEmptyList(s, ss: _*).success // No Failures collected!
              case (s :: ss, f :: fs) =>  NonEmptyList(f, fs: _*).fail    // Some Failures, return those. Will only happen if the event fails validation in jsonToRawEvent
              case (Nil,           _) => "List of events is empty (should never happen, not catching empty list properly)".failNel
            }
          }
        }
      }
    }

  /**
   * Fabricates a Validated RawEvent from a
   * Collecter Payload and an Event JSON.
   *  
   * To return a RawEvent we need to:
   * - Validate that the event JSON has an 
   *   event parameter
   * - Validate that the event parameter 
   *   within the JSON returns a valid schema
   *   URI
   *
   * @param payload The CollectorPayload parameters 
   *        which we will nest into the RawEvent
   * @param json The event JSON we will be nesting
   *        into the RawEvent
   * @return a RawEvent containing the payload 
   *         and JSON information or a Failure Nel
   */
  def jsonToRawEvent(payload: CollectorPayload, json: JValue): Validated[RawEvent] =
    extractKeyValueFromJson("event", json) match {
      case None => s"Mandrill event parameter not provided: cannot determine event type".failNel
      case Some(eventType) => {

        for {
          schema <- (lookupSchema(eventType).toValidationNel: Validated[String])
        } yield {
          val qsParams = toMap(payload.querystring)
          val formattedJson = reformatParameters(json)
          RawEvent(
            api          = payload.api,
            parameters   = toUnstructEventParamsMandrill(TrackerVersion, qsParams, formattedJson, schema, "srv"),
            contentType  = payload.contentType,
            source       = payload.source,
            context      = payload.context
          )
        }
      }
    }

  /**
   * Fabricates a Snowplow unstructured event from
   * the supplied parameters. Note that to be a
   * valid Snowplow unstructured event, the event
   * must contain e, p and tv parameters, so we
   * make sure to set those.
   *
   * @param tracker The name and version of this
   *        tracker
   * @param qsParams The query-string parameters
   *        we will nest into the unstructured event
   * @param schema The schema key which defines this
   *        unstructured event as a String
   * @param formatter A function to take the raw event
   *        parameters and turn them into a correctly
   *        formatted JObject that should pass JSON
   *        Schema validation
   * @param platform The default platform to assign
   *         the event to
   * @return the raw-event parameters for a valid
   *         Snowplow unstructured event
   */
  private[registry] def toUnstructEventParamsMandrill(tracker: String, params: RawEventParameters, eventJson: JValue, 
    schema: String, platform: String = "app"): RawEventParameters = {

    val json = compact {
      ("schema" -> SchemaUris.UnstructEvent) ~
      ("data"   -> (
        ("schema" -> schema) ~
        ("data"   -> eventJson)
      ))
    }

    Map(
      "tv"    -> tracker,
      "e"     -> "ue",
      "p"     -> params.getOrElse("p", platform), // Required field
      "ue_pr" -> json) ++
    params.filterKeys(Set("nuid", "aid", "cv"))
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
  private[registry] def bodyToEventList(rawEventString: String): Validation[String,List[JValue]] = {

    val bodyMap = toMap(URLEncodedUtils.parse(URI.create("http://localhost/?" + rawEventString), "UTF-8").toList)

    bodyMap match {
      case map if map.size != 1 => s"Mapped Mandrill body has invalid count of keys: ${map.size}".fail
      case map                  => {
        map.get("mandrill_events") match {
          case None       => s"Mapped Mandrill body does not have 'mandrill_events' as a key".fail
          case Some("")   => s"Mandrill events string is empty: nothing to process".fail
          case Some(dStr) => {
            try {
              val parsed = parse(dStr)
              parsed match {
                case JArray(list) => list.success
                case _            => s"Could not resolve Mandrill payload into a JSON array of events".fail
              }
            } catch {
              case e: JsonParseException => {
                val exception = JU.stripInstanceEtc(e.toString)
                s"Mandrill events string failed to parse into JSON: [$exception]".fail
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
   *
   * @param json The event JSON which we need to
   *        update values for
   * @return the updated JSON with valid date-time
   *         values in the 'ts' fields
   */
  private[registry] def reformatParameters(json: JValue): JValue =
    json transformField {
      case ("ts", JInt(x)) => {
        try {
          val dt: DateTime = new DateTime(x.longValue() * 1000)
          ("ts", JString(JsonSchemaDateTimeFormat.print(dt)))
        } catch {
          case _ : Throwable => ("ts", JInt(x))
        }
      }
    }

  /**
   * Extracts the value of a key from a json4s JSON
   * - Will return either an Option[String] if the key
   *   is valid or None if the key is not valid or it
   *   cannot convert the value found into a String
   *
   * @param key The key pertaining to the value we 
   *        want returned from the event JSON
   * @param json The JValue JSON we want to get a 
   *        value from
   * @return an Option[String] with the value we want or
   *         None
   */
  private[registry] def extractKeyValueFromJson(key: String, json: JValue): Option[String] = 
    (json \ key).extractOpt[String]

  /**
   * Gets the correct Schema URI for the event passed from Mandrill
   *
   * @param eventType The string pertaining to the type 
   *        of event schema we are looking for
   * @return the schema for the event or a Failure-boxed String
   *         if we can't recognize the event type
   */
  private[registry] def lookupSchema(eventType: String): Validation[String, String] = 
    eventType match {
      case "hard_bounce" => SchemaUris.MessageBounced.success
      case "click"       => SchemaUris.MessageClicked.success
      case "deferral"    => SchemaUris.MessageDelayed.success
      case "spam"        => SchemaUris.MessageMarkedAsSpam.success
      case "open"        => SchemaUris.MessageOpened.success
      case "reject"      => SchemaUris.MessageRejected.success
      case "send"        => SchemaUris.MessageSent.success
      case "soft_bounce" => SchemaUris.MessageSoftBounced.success
      case "unsub"       => SchemaUris.RecipientUnsubscribed.success
      case ""            => s"Mandrill event parameter is empty: cannot determine event type".fail
      case et            => s"Mandrill event parameter [$et] not recognized".fail
    }
}