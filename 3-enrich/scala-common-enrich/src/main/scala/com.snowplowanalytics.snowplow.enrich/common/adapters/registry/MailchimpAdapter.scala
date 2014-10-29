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
    // Get the Query string parameters
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
    val bodyMap = toMap(URLEncodedUtils.parse(URI.create("http://localhost/?" + body), "UTF-8").toList)
    val schema = getSchemaKeyString(bodyMap.get("type"))
    val parameters = qsParams ++ bodyMap

    println(parameters)

    val tempMap = scala.collection.mutable.Map[Array[String],String]()

    parameters foreach {
      case (key, value) => tempMap(key.split("\\]?\\[")) = value
    }

    //data={"action":"unsub","merges":{"FNAME":"Joshua"}}
    //List(tv=0, nuid=123)
    /*Map(
      data[ip_opt] -> 82.225.169.220, 
      data[merges][LNAME] -> Beemster, 
      tv -> 0, 
      data[email] -> josh@snowplowanalytics.com, 
      data[list_id] -> f1243a3b12, 
      nuid -> 123, 
      data[email_type] -> html, 
      data[reason] -> manual, 
      data[id] -> 94826aa750, 
      data[merges][FNAME] -> Joshua, 
      fired_at -> 2014-10-22 13:10:40, 
      data[action] -> unsub, 
      type -> unsubscribe, 
      data[web_id] -> 203740265, 
      data[merges][EMAIL] -> josh@snowplowanalytics.com
    )*/

    val finalMap = scala.collection.mutable.Map[String,Any]()
    val mapObj = scala.collection.mutable.Map[String,Any]()

    tempMap foreach { case (keys, value) => 
      val count = keys.length

      if (count > 1) {
        val tempList = new ArrayList(0)
        var x = 0

        for (x <- 0 to count-1) {
          var key = keys(x)
          if (!finalMap.contains(key)) {
            var toAdd = mapObj

            if (x == 0) {
              finalMap += key -> toAdd
            }
            else if (x == 1) {
              finalMap += key -> toAdd
            }
          }
        }
      }
      else {
        var key = keys(0)
        finalMap += key -> value
      }
    }

    println(finalMap)

    val params = compact {
      ("schema" -> SchemaUris.UnstructEvent) ~
      ("data"   -> (
        ("schema" -> schema) ~
        ("data"   -> (parameters - ("nuid", "aid", "cv", "p")))
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
   * Gets the correct Schema URI for the event passed from Mailchimp
   */
  private def getSchemaKeyString(eventType: Option[String]) : String = {
    eventType match {
      case Some(event) if event == "subscribe" => return SchemaUris.Subscribe
      case Some(event) if event == "unsubscribe" => return SchemaUris.Unsubscribe
      case Some(event) if event == "campaign" => return SchemaUris.CampaignSendingStatus
      case Some(event) if event == "cleaned" => return SchemaUris.CleanedEmail
      case Some(event) if event == "upemail" => return SchemaUris.EmailAddressChange
      case Some(event) if event == "profile" => return SchemaUris.ProfileUpdate
      case Some(_) => return s"Bad event type sent"
    }
  }
}
