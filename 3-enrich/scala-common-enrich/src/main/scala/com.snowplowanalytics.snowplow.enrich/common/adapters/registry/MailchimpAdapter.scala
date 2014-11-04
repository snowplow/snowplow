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

/**
 * Transforms a collector payload which conforms to
 * a known version of the Mailchimp Tracking webhook
 * into raw events.
 */
object MailchimpAdapter extends Adapter {
  // Tracker version for an Mailchimp Tracking webhook
  private val TrackerVersion = "com.mailchimp-v1"

  // Expected content type for a request body
  private val ContentType = "application/x-www-form-urlencoded; charset=utf-8"

  // DateTime Formatters
  private val DtfIn:  DateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").withZone(DateTimeZone.UTC)
  private val DtfOut: DateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss'Z'").withZone(DateTimeZone.UTC)

  // Schemas for reverse-engineering a Snowplow unstructured event
  private object SchemaUris {
    val UnstructEvent         = SchemaKey("com.snowplowanalytics.snowplow", "unstruct_event", "jsonschema", "1-0-0").toSchemaUri
    val Subscribe             = SchemaKey("com.mailchimp", "subscribe", "jsonschema", "1-0-0").toSchemaUri
    val Unsubscribe           = SchemaKey("com.mailchimp", "unsubscribe", "jsonschema", "1-0-0").toSchemaUri
    val ProfileUpdate         = SchemaKey("com.mailchimp", "profile_update", "jsonschema", "1-0-0").toSchemaUri
    val EmailAddressChange    = SchemaKey("com.mailchimp", "email_address_change", "jsonschema", "1-0-0").toSchemaUri
    val CleanedEmail          = SchemaKey("com.mailchimp", "cleaned_email", "jsonschema", "1-0-0").toSchemaUri
    val CampaignSendingStatus = SchemaKey("com.mailchimp", "campaign_sending_status", "jsonschema", "1-0-0").toSchemaUri
  }

  // Formatter Function to convert RawEventParameters into a merged Json Object
  private val MailchimpFormatter: FormatterFunc = {
    (parameters: RawEventParameters) => mergeJObjects(getJsonObject(parameters))
  }

  /**
   * Converts a CollectorPayload instance into raw events.
   * An Mailchimp Tracking payload only contains a single event.
   * We expect the name parameter to be 1 of 6 options otherwise
   * we have an unsupported event type.
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
      case None       => s"Request body is empty: no MailChimp event to process".failNel
      case Some(body) => {

        val params = toMap(URLEncodedUtils.parse(URI.create("http://localhost/?" + body), "UTF-8").toList)
        params.get("type") match {
          case None => s"No MailChimp type parameter provided: cannot determine event type".failNel
          case Some(eventType) => {

            val allParams = toMap(payload.querystring) ++ reformatBadParamValues(params)
            for {
              schema <- (lookupSchema(eventType).toValidationNel: Validated[String])
            } yield {
              NonEmptyList(RawEvent(
                api          = payload.api,
                parameters   = toUnstructEventParams(TrackerVersion, allParams, schema, MailchimpFormatter, "srv"),
                contentType  = payload.contentType,
                source       = payload.source,
                context      = payload.context
              ))
            }
          }
        }
      }
    }

  /**
   * Generates a list of Json Objects from the body of the event
   * 
   * @param paramMap The map of all the parameters 
   *        for this event 
   * @return Iterable[JObject] Generates a list of 
   *         JObjects which represents every value 
   *         in the JSON with its own full path
   */
  private def getJsonObject(paramMap: Map[String,String]): Iterable[JObject] = {
    for {
      (k, v) <- paramMap
    } yield (recurse(toKeys(k), v))
  }

  /**
   * Returns a list of keys from a string
   * 
   * @param formKey The Key String that (may) need to be split.
   * @return List[String] Generates a list of string based on 
   *         the below regex
   */
  private def toKeys(formKey: String): List[String] = 
    formKey.split("\\]?(\\[|\\])").toList

  /**
   * Recursively generates a correct Json Object
   *
   * @param keys The list of Keys generated from toKeys()
   * @param nestedMap The value for the list of keys
   * @return JObject Generates a JObject out of a list of 
   *         keys and a value
   */
  private def recurse(keys: List[String], nestedMap: String): JObject =
    keys match {
      case head :: tail if tail.size == 0 => JObject(head -> JString(nestedMap))
      case head :: tail                   => JObject(head -> recurse(tail, nestedMap))
    }

  /**
   * Merges all of the JObjects together

   * @param listOfJObjects The list of JObjects which needs to be
   *        merged together
   * @return JObject Creates a fully merged JObject from the List 
   *         provided
   */
  private def mergeJObjects(listOfJObjects: Iterable[JObject]): JObject = 
    listOfJObjects match {
      case head :: tail => tail.foldLeft(head)(_ merge _)
    }

  /**
   * Fixes the parameter values of the event
   *
   * @param params The parameters to be checked for fixing
   * @return RawEventParameters Either with a fixed Date Time 
   *         if the key was found or the original parameters
   */
  private def reformatBadParamValues(params: RawEventParameters): RawEventParameters =
    params.get("fired_at") match {
      case Some(param) => params.updated("fired_at", reformateDateTimeForJsonSchema(param))
      case None        => params
    }

  /**
   * Return the date time formatted correctly for JSON
   *
   * @param value The datetime string to be formatted
   * @return String Correctly Formatted DateTime
   */
  private def reformateDateTimeForJsonSchema(value: String): String =
    DtfOut.print(DateTime.parse(value, DtfIn))

  /**
   * Gets the correct Schema URI for the event passed from Mailchimp

   * @param eventType The string pertaining to the type 
   *        of event schema we are looking for
   * @return the schema for the event or a Failure-boxed String
   *         if we can't recognize the event type
   */
  private def lookupSchema(eventType: String): Validation[String, String] = eventType match {
    case "subscribe"   => SchemaUris.Subscribe.success
    case "unsubscribe" => SchemaUris.Unsubscribe.success
    case "campaign"    => SchemaUris.CampaignSendingStatus.success
    case "cleaned"     => SchemaUris.CleanedEmail.success
    case "upemail"     => SchemaUris.EmailAddressChange.success
    case "profile"     => SchemaUris.ProfileUpdate.success
    case ""            => s"MailChimp type parameter is empty: cannot determine event type".fail
    case et            => s"MailChimp type parameter [$et] not recognized".fail
  }
}
