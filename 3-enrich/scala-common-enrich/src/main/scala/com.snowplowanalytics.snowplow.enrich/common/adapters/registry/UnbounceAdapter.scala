/*
 * Copyright (c) 2016 Snowplow Analytics Ltd. All rights reserved.
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
import org.apache.http.NameValuePair

// Scala
import scala.util.matching.Regex
import scala.collection.JavaConversions._

// Scalaz
import scalaz._
import Scalaz._

// Jackson
import com.fasterxml.jackson.core.JsonParseException

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
 * a known version of the Unbounce Tracking webhook
 * into raw events.
 */
object UnbounceAdapter extends Adapter {

  // Vendor name for Failure Message
  private val VendorName = "Unbounce"

  // Tracker version for an Unbounce Tracking webhook
  private val TrackerVersion = "com.unbounce-v1"

  // Expected content type for a request body
  private val ContentType = "application/x-www-form-urlencoded"

  private val AcceptedQueryParameters = Set("nuid", "aid", "cv", "eid", "ttm", "url")

  // Schema for Unbounce event context
  private val ContextSchema = SchemaKey("com.unbounce", "event_context", "jsonschema", "1-0-0").toSchemaUri

  /**
   * Converts a CollectorPayload instance into raw events.
   *
   * An Unbounce Tracking payload contains one single event
   * in the body of the payload, stored within a HTTP encoded
   * string.
   *
   * @param payload The CollectorPayload containing one or more
   *        raw events as collected by a Snowplow collector
   * @param resolver (implicit) The Iglu resolver used for
   *        schema lookup and validation. Not used
   * @return a Validation boxing either a NEL of RawEvents on
   *         Success, or a NEL of Failure Strings
   */
  def toRawEvents(payload: CollectorPayload)(implicit resolver: Resolver): ValidatedRawEvents = {
    val params = toMap(payload.querystring)
    (params.get("schema"), payload.body, payload.contentType) match {
      case (Some(schemaUri), None, _) => s"Request body is empty: no ${VendorName} events to process".failNel
      case (_, Some(body), None) => s"Request body provided but content type empty, expected ${ContentType} for ${VendorName}".failNel
      case (_, _, Some(ct)) if ct != ContentType => s"Content type of ${ct} provided, expected ${ContentType} for ${VendorName}".failNel
      case (Some(schemaUri), Some(body), Some(ct)) => {
        if (body.isEmpty) {
          s"${VendorName} event body is empty: nothing to process".failNel
        } else {
          try {
            val bodyMap = toMap(URLEncodedUtils.parse(URI.create("http://localhost/?" + body), "UTF-8").toList)
            (payloadBodyToEvent(bodyMap), payloadBodyToContext(bodyMap)) match {
              case (Failure(err), _) => err.failNel
              case (_, Failure(err)) => err.failNel
              case (Success(event), Success(context)) => {
                SchemaKey.parse(schemaUri) match {
                  case Failure(procMsg) => procMsg.getMessage.failNel
                  case Success(_) => {
                    NonEmptyList(RawEvent(
                      api         = payload.api,
                      parameters  = toUnstructEventParams(TrackerVersion, params, schemaUri, event, context, "srv"),
                      contentType = payload.contentType,
                      source      = payload.source,
                      context     = payload.context
                    )).success
                  }
                }
              }
            }
          } catch { 
            case e: Exception => {
              val exception = JU.stripInstanceEtc(e.toString).orNull
              s"${VendorName} could not parse body: [${exception}]".failNel
            }
          }
        }
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
   * @param eventJson The event which we will nest
   *        into the unstructured event
   * @param contextJson The context which we will nest
   *        into the unstructured event
   * @param platform The default platform to assign
   *        the event to
   * @return the raw-event parameters for a valid
   *         Snowplow unstructured event
   */
  private def toUnstructEventParams(tracker: String, qsParams: RawEventParameters, schema: String,
    eventJson: JObject, contextJson: JObject, platform: String): RawEventParameters = {

    val eventDataJson = compact {
      toUnstructEvent(
        ("schema" -> schema) ~
        ("data"   -> eventJson)
      )
    }

    val contextDataJson = compact {
      toContexts(
        ("schema" -> ContextSchema) ~
        ("data"   -> contextJson)
      )
    }

    Map(
      "tv"    -> tracker,
      "e"     -> "ue",
      "p"     -> qsParams.getOrElse("p", platform),
      "ue_pr" -> eventDataJson,
      "co"    -> contextDataJson) ++
    qsParams.filterKeys(AcceptedQueryParameters)
  }

  /**
   * Converts a querystring payload into an event
   * @param bodyMap The converted map from the querystring
   */
  private def payloadBodyToEvent(bodyMap: Map[String, String]): Validation[String, JObject] = {
    bodyMap.get("data.json") match {
      case None       => s"${VendorName} event data does not have 'data.json' as a key".fail
      case Some("")   => s"${VendorName} event data is empty: nothing to process".fail
      case Some(json) => {
        try {
          val eventDataJson = json.replaceAll("\\[|\\]","")
          val event = parse(eventDataJson)
          event match {
            case obj: JObject => obj.success
            case _            => s"${VendorName} event wrong type: [%s]".format(event.getClass).fail
          }
        } catch {
          case e: JsonParseException => {
              val exception = JU.stripInstanceEtc(e.toString).orNull
              s"${VendorName} event string failed to parse into JSON: [${exception}]".fail
          }
          case e: Exception => {
            val exception = JU.stripInstanceEtc(e.toString).orNull
            s"${VendorName} incorrect event string : [${exception}]".fail
          }
        } 
      }
    }
  }

  /**
   * Converts a querystring payload into a context
   * @param bodyMap The converted map from the querystring
   */
  private def payloadBodyToContext(bodyMap: Map[String, String]): Validation[String, JObject] = {
    (bodyMap.get("page_id"), bodyMap.get("page_name"), 
     bodyMap.get("variant"), bodyMap.get("page_url")) match {
      case (None, _, _, _) => s"${VendorName} context data missing 'page_id'".fail
      case (_, None, _, _) => s"${VendorName} context data missing 'page_name'".fail
      case (_, _, None, _) => s"${VendorName} context data missing 'variant'".fail
      case (_, _, _, None) => s"${VendorName} context data missing 'page_url'".fail
      case (Some(page_id), Some(page_name), Some(variant), Some(page_url)) => {
        val context: JObject = ("page_id" -> page_id) ~ 
                               ("page_name" -> page_name) ~ 
                               ("variant" -> variant) ~ 
                               ("page_url" -> page_url)
        context.success
      }
    }
  }

}
