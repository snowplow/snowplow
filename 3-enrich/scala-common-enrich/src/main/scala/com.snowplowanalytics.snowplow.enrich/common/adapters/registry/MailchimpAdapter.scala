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
import java.util.Map.{Entry => JMapEntry}
import java.util.ArrayList
import java.net.URI
import java.net.URLDecoder
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

// This project
import loaders.CollectorPayload
import utils.JsonUtils

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

  // Schemas for reverse-engineering a Snowplow unstructured event
  private object SchemaUris {
    val UnstructEvent = SchemaKey("com.snowplowanalytics.snowplow", "unstruct_event", "jsonschema", "1-0-0").toSchemaUri
    val Subscribe = SchemaKey("com.mailchimp", "subscribe", "jsonschema", "1-0-0").toSchemaUri
    val Unsubscribe = SchemaKey("com.mailchimp", "unsubscribe", "jsonschema", "1-0-0").toSchemaUri
    val ProfileUpdate = SchemaKey("com.mailchimp", "profile_update", "jsonschema", "1-0-0").toSchemaUri
    val EmailAddressChange = SchemaKey("com.mailchimp", "email_address_change", "jsonschema", "1-0-0").toSchemaUri
    val CleanedEmail = SchemaKey("com.mailchimp", "cleaned_email", "jsonschema", "1-0-0").toSchemaUri
    val CampaignSendingStatus = SchemaKey("com.mailchimp", "campaign_sending_status", "jsonschema", "1-0-0").toSchemaUri
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
  def toRawEvents(payload: CollectorPayload)(implicit resolver: Resolver): ValidatedRawEvents = {
    val qsParams = toMap(payload.querystring)
    payload.body match {
      case (None) if qsParams.isEmpty => s"Request body and querystring parameters empty, expected at least one populated".failNel
      case (None)                     => s"Event body empty".failNel
      case (Some(body))               => // Build our NEL of parameters
        NonEmptyList(RawEvent(
          api          = payload.api,
          parameters   = toUnstructEventParams(TrackerVersion, qsParams, body),
          contentType  = payload.contentType,
          source       = payload.source,
          context      = payload.context
          )).success
      }
  }

  /**
   * Fabricates a Snowplow unstructured event from
   * the supplied parameters setup git in ubuntu. Note that to be a
   * valid Snowplow unstructured event, the event
   * must contain e, p and tv parameters, so we
   * make sure to set those.
   *
   * @param tracker The name and version of this
   *        tracker
   * @param parameters The raw-event parameters
   *        we will nest into the unstructured event
   * @param schema The schema for the event passed
   * @return the raw-event parameters for a valid
   *         Snowplow unstructured event
   */
  private def toUnstructEventParams(tracker: String, qsParams: RawEventParameters, body: String): RawEventParameters = {
    val parameters = ((toMap(URLEncodedUtils.parse(URI.create("http://localhost/?" + body), "UTF-8").toList) 
                       ++ qsParams) - ("nuid", "aid", "cv", "p"))
    val myJsonObject = mergeJObjects(getJsonObject(parameters))
    val schema = getSchema(parameters.get("type"))
    val params = compact {
      ("schema" -> SchemaUris.UnstructEvent) ~
      ("data"   -> (
        ("schema" -> schema) ~
        ("data"   -> myJsonObject)
      ))
    }
    Map(
      "tv"    -> tracker,
      "e"     -> "ue",
      "p"     -> parameters.getOrElse("p", "app"),
      "ue_pr" -> params
    ) ++ parameters.filterKeys(Set("nuid", "aid", "cv"))
  }

  /**
   * Generates a list of maps from the body of the event
   * 
   * @param paramMap The map of all the parameters 
   *                 for this event 
   */
  def getJsonObject(paramMap: Map[String,String]) =
    for {
      (k, v) <- paramMap
    } yield (recurse(toKeys(k), v))

  /**
   * Returns a list of keys from a string
   * 
   * @param formKey The Key String that (may) need to be split.
   *                Will return either a list of multiple keys 
   *                or a list with a single key if no splitting 
   *                was required.
   */
  def toKeys(formKey: String): List[String] = 
    formKey.split("\\]?(\\[|\\])").toList

  /**
   * Recursively generates a correct Json Object
   *
   * @param keys The list of Keys generated from toKeys()
   * @param nestedMap The value for the list of keys
   */
  def recurse(keys: List[String], nestedMap: String): JObject =
    keys match {
      case Nil => throw new RuntimeException("This should never happen - recurse error.")
      case head :: tail if tail.size == 0 => JObject(head -> JString(nestedMap))
      case head :: tail => JObject(head -> recurse(tail, nestedMap))
    }

  /**
   * Merges all of the JObjects together

   * @param listOfJObjects The list of JObjects which needs to be
   *                       merged together
   */
  def mergeJObjects(listOfJObjects: Iterable[JObject]): JObject = 
    listOfJObjects match {
      case Nil => throw new RuntimeException("This should never happen - mergeJObjects error.")
      case head :: tail => tail.foldLeft(head)(_ merge _)
    }

  /**
   * Gets the correct Schema URI for the event passed from Mailchimp

   * @param eventType The string pertaining to the type 
   *                  of event schema we are looking for
   */
  def getSchema(eventType: Option[String]) : String = {
    eventType match {
      case Some(event) if event == "subscribe" => return SchemaUris.Subscribe
      case Some(event) if event == "unsubscribe" => return SchemaUris.Unsubscribe
      case Some(event) if event == "campaign" => return SchemaUris.CampaignSendingStatus
      case Some(event) if event == "cleaned" => return SchemaUris.CleanedEmail
      case Some(event) if event == "upemail" => return SchemaUris.EmailAddressChange
      case Some(event) if event == "profile" => return SchemaUris.ProfileUpdate
      case Some(_) => throw new RuntimeException("Invalid Event Type specified.")
    }
  }
}
