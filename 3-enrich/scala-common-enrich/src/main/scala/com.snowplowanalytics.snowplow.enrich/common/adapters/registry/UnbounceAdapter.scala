/*
 * Copyright (c) 2016-2018 Snowplow Analytics Ltd. All rights reserved.
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

// Scala
import scala.util.{Try, Success => TS, Failure => TF}
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

// Iglu
import iglu.client.{Resolver, SchemaKey}

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
  private val ContextSchema = Map(
    "form_post" -> SchemaKey("com.unbounce", "form_post", "jsonschema", "1-0-0").toSchemaUri
  )

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
  def toRawEvents(payload: CollectorPayload)(implicit resolver: Resolver): ValidatedRawEvents =
    (payload.body, payload.contentType) match {
      case (None, _) => s"Request body is empty: no ${VendorName} events to process".failureNel
      case (_, None) =>
        s"Request body provided but content type empty, expected ${ContentType} for ${VendorName}".failureNel
      case (_, Some(ct)) if ct != ContentType =>
        s"Content type of ${ct} provided, expected ${ContentType} for ${VendorName}".failureNel
      case (Some(body), _) =>
        if (body.isEmpty) s"${VendorName} event body is empty: nothing to process".failureNel
        else {
          val qsParams = toMap(payload.querystring)
          Try { toMap(URLEncodedUtils.parse(URI.create("http://localhost/?" + body), "UTF-8").toList) } match {
            case TF(e) =>
              s"${VendorName} incorrect event string : [${JU.stripInstanceEtc(e.getMessage).orNull}]".failureNel
            case TS(bodyMap) =>
              payloadBodyToEvent(bodyMap).flatMap {
                case event => {
                  lookupSchema(Some("form_post"), VendorName, ContextSchema).flatMap {
                    case schema: String =>
                      toUnstructEventParams(TrackerVersion, qsParams, schema, event, "srv") match {
                        case unstructEventParams =>
                          NonEmptyList(
                            RawEvent(
                              api         = payload.api,
                              parameters  = unstructEventParams,
                              contentType = payload.contentType,
                              source      = payload.source,
                              context     = payload.context
                            )).success
                      }
                  }
                }
              }
          }
        }
    }

  private def payloadBodyToEvent(bodyMap: Map[String, String]): Validated[JValue] =
    (bodyMap.get("page_id"),
     bodyMap.get("page_name"),
     bodyMap.get("variant"),
     bodyMap.get("page_url"),
     bodyMap.get("data.json")) match {
      case (None, _, _, _, _) => s"${VendorName} context data missing 'page_id'".failureNel
      case (_, None, _, _, _) => s"${VendorName} context data missing 'page_name'".failureNel
      case (_, _, None, _, _) => s"${VendorName} context data missing 'variant'".failureNel
      case (_, _, _, None, _) => s"${VendorName} context data missing 'page_url'".failureNel
      case (_, _, _, _, None) => s"${VendorName} event data does not have 'data.json' as a key".failureNel
      case (_, _, _, _, Some(dataJson)) if dataJson.isEmpty =>
        s"${VendorName} event data is empty: nothing to process".failureNel
      case (Some(pageId), Some(pageName), Some(variant), Some(pageUrl), Some(dataJson)) => {
        try {
          val event = parse(compact(render(bodyMap - "data.json")))
          camelize(
            ("data.json", parse(dataJson)) :: event
              .filterField({ case (name: String, _) => name != "data.xml" })).success
        } catch {
          case e: JsonParseException =>
            s"${VendorName} event string failed to parse into JSON: [${JU.stripInstanceEtc(e.getMessage).orNull}]".failureNel
        }
      }
    }
}
