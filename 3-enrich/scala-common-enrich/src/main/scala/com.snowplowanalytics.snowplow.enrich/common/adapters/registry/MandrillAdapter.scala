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

  // Datetime format used by Mandrill (as we will need to massage) - [CHECK THIS]
  private val MandrillDateTimeFormat = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").withZone(DateTimeZone.UTC)

  /**
   * Converts a CollectorPayload instance into raw events.
   *
   * A Mandrill Tracking payload contains many events in a JSON structure
   * We expect the event parameter to be 1 of 9 options otherwise we have 
   * an unsupported event type.
   *
   * @param payload The CollectorPayload containing one or more
   *        raw events as collected by a Snowplow collector
   * @param resolver (implicit) The Iglu resolver used for
   *        schema lookup and validation. Not used
   * @return a Validation boxing either a NEL of RawEvents on
   *         Success, or a NEL of Failure Strings
   */
  def toRawEvents(payload: CollectorPayload)(implicit resolver: Resolver): ValidatedRawEvents =
    payload.body match {
      case None       => s"Request body is empty: no Mandrill events to process".failNel
      case Some(body) if !body.startsWith("mandrill_events=") => s"Request body is formatted incorrectly: cannot process".failNel
      case Some(body) => {

        val eventList = eventStringToList(body)
        val rawEventsList: List[Validation[NonEmptyList[String],RawEvent]] = { //List[Validation[NonEmptyList[String],RawEvent]]
          for { 
            event <- eventList // List[JValue]
          } 
          yield {
            extractKeyValueFromJson("event", event) match { // Option[String] || None
              case None => s"No Mandrill event parameter provided: cannot determine event type".failNel
              case Some(eventType) => {

                val params = toMap(payload.querystring) // RawEventParameters
                for {
                  schema <- (lookupSchema(eventType).toValidationNel: Validated[String]) // Validation[NonEmptyList[String],String]
                } yield {
                  RawEvent(
                    api          = payload.api,
                    parameters   = toUnstructEventParamsMandrill(TrackerVersion, params, event, schema, "srv"), // RawEventParameters
                    contentType  = payload.contentType,
                    source       = payload.source,
                    context      = payload.context
                  )
                }
              }
            }
          }
        }
        
        // Gather all of our successes and failures into seperate lists.
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

        // Send out our ValidatedRawEvents (either a Nel of failures or a Nel of RawEvents)
        (successes, failures) match {
          case (s :: ss, Nil)     =>  NonEmptyList(s, ss: _*).success // No Failures collected
          case (s :: ss, f :: fs) =>  NonEmptyList(f, fs: _*).fail    // Some Failures, return those. Should never happen, unless JSON Schema changed
          case (Nil,     _)       => "List of events is empty (should never happen, did JSON Schema change?)".failNel
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
   * Returns a list of JValue formatted JSONs from 
   * the Mandrill Event String
   * - Should return at least one event
   *
   * @param rawEventString The http encoded string 
   *        from the Mandrill POST event body
   * @return a list of single events formatted as 
   *         json4s JValue JSONs
   */
  private[registry] def eventStringToList(rawEventString: String): List[JValue] = {
    val eventStr = rawEventString.replace("mandrill_events=","") //TODO: Slightly less heinous but still pretty bad
    val parsedStr = parse(URLEncodedUtils.parse(URI.create("http://localhost/?" + eventStr), "UTF-8").toString)
    for {
      JArray(List(JArray(x))) <- parsedStr
      event <- x
    } yield event
  }

  /**
   * Extracts the value of a key from a json4s JObject
   * TODO - Better case analysis for returned type
   *
   * @param key The key pertaining to the value we 
  *         want returned from the event JSON
   * @param event A single JSON Event from Mandrill
   * @return the value for the key or none
   */
  private[registry] def extractKeyValueFromJson(key: String, event: JValue): Option[String] = 
    (event \ key).extractOpt[String]

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