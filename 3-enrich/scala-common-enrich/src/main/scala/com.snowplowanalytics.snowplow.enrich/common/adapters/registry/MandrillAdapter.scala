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
  // Tracker version for an Mandrill Tracking webhook
  private val TrackerVersion = "com.mandrill-v1"

  // Expected content type for a request body - [CHECK THIS]
  private val ContentType = "application/x-www-form-urlencoded; charset=utf-8"

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

  // Formatter Function to convert RawEventParameters into a merged Json Object
  private val MandrillFormatter: FormatterFunc = {
    (parameters: RawEventParameters) => JObject(List(("data",JObject(List(("merges",JObject(List(("LNAME",JString("Beemster"))))))))))
  }

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
      case Some(body) => {

        // STEPS TO PROCESS MANDRILL EVENTS
        // 1. Trim "mandrill_events=" from the front of the body payload
        // 2. Parse remaining into a valid json string
        //    - Function: URLEncodedUtils.parse(URI.create("http://localhost/?" + body), "UTF-8").toString
        // 3. Split the json blocks inside into seperate events
        // 4. Process each event seperately and return the ValidatedRawEvents

        val eventsList = eventsStringToList(body)

        // Loop through events and add them to the NonEmptyList
        for {
          event <- eventsList
        } 
        yield {
          // Check the type of the event, if it validates continue
          
        }


        val params = toMap(URLEncodedUtils.parse(URI.create("http://localhost/?" + body), "UTF-8").toList)

        // Need to case match against all events in the body payload here
        // Need to add query string params against all events sent.

        params.get("event") match {
          case None => s"No Mandrill event parameter provided: cannot determine event type".failNel
          case Some(eventType) => {

            val allParams = toMap(payload.querystring) ++ params
            for {
              schema <- (lookupSchema(eventType).toValidationNel: Validated[String])
            } yield {
              NonEmptyList(RawEvent(
                api          = payload.api,
                parameters   = toUnstructEventParams(TrackerVersion, allParams, schema, MandrillFormatter, "srv"),
                contentType  = payload.contentType,
                source       = payload.source,
                context      = payload.context
              ))
            }
          }
        }
      }
    }
  
  def eventStringToList(rawEventsString: String): List[JObject] = {
    val eventStr = rawEventsString.drop(16) // Drop mandrill_events= from string
    val parsedStr = parse(URLEncodedUtils.parse(URI.create("http://localhost/?" + eventStr), "UTF-8").toString) // Convert string into JObject
    for {
      JArray(List(JArray(x))) <- parsedStr
      listOfEvents <- x
    } yield listOfEvents 
  }

  def returnJStringValForKey(key: String, event: JValue): String =
    (event \ key).extractOpt[String]

  /**
   * Gets the correct Schema URI for the event passed from Mandrill
   *
   * @param eventType The string pertaining to the type 
   *        of event schema we are looking for
   * @return the schema for the event or a Failure-boxed String
   *         if we can't recognize the event type
   */
  private[registry] def lookupSchema(eventType: String): Validation[String, String] = eventType match {
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
