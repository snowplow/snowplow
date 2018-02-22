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
import org.joda.time.DateTime

// Scala
import scala.util.matching.Regex
import scala.util.control.NonFatal
import scala.collection.JavaConversions._
import scala.util.{Try, Success => TS, Failure => TF}

// Scalaz
import scalaz._
import Scalaz._

// Jackson
import com.fasterxml.jackson.core.JsonParseException

// json4s
import org.json4s._
import org.json4s.jackson.JsonMethods._

// Iglu
import iglu.client.{Resolver, SchemaKey}

// This project
import loaders.CollectorPayload
import utils.{JsonUtils => JU}

/**
 * Transforms a collector payload which conforms to
 * a known version of the Olark Tracking webhook
 * into raw events.
 */
object OlarkAdapter extends Adapter {

  // Vendor name for Failure Message
  private val VendorName = "Olark"

  // Tracker version for an Olark Tracking webhook
  private val TrackerVersion = "com.olark-v1"

  // Expected content type for a request body
  private val ContentType = "application/x-www-form-urlencoded"

  // Schemas for reverse-engineering a Snowplow unstructured event
  private val EventSchemaMap = Map(
    "transcript"      -> SchemaKey("com.olark", "transcript", "jsonschema", "1-0-0").toSchemaUri,
    "offline_message" -> SchemaKey("com.olark", "offline_message", "jsonschema", "1-0-0").toSchemaUri
  )

  /**
   * Converts a CollectorPayload instance into raw events.
   *
   * An Olark Tracking payload contains one single event
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
      case (Some(body), _) if (body.isEmpty) => s"${VendorName} event body is empty: nothing to process".failureNel
      case (Some(body), _) => {
        val qsParams = toMap(payload.querystring)
        Try { toMap(URLEncodedUtils.parse(URI.create("http://localhost/?" + body), "UTF-8").toList) } match {
          case TF(e) => s"${VendorName} could not parse body: [${JU.stripInstanceEtc(e.getMessage).orNull}]".failureNel
          case TS(bodyMap) =>
            payloadBodyToEvent(bodyMap).flatMap {
              case event => {
                val eventType = (event \ "operators") match {
                  case (JNothing) => Some("offline_message")
                  case (_)        => Some("transcript")
                }
                lookupSchema(eventType, VendorName, EventSchemaMap).flatMap {
                  case schema =>
                    transformTimestamps(event).flatMap {
                      case transformedEvent =>
                        NonEmptyList(
                          RawEvent(
                            api = payload.api,
                            parameters = toUnstructEventParams(TrackerVersion,
                                                               qsParams,
                                                               schema,
                                                               camelize(transformedEvent),
                                                               "srv"),
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

  /**
   * Converts all olark timestamps in a parsed transcript or offline_message json object to iso8601 strings
   *
   * @param json a parsed event
   * @return JObject the event with timstamps replaced
   */
  private def transformTimestamps(json: JValue): Validated[JValue] = {
    def toMsec(oTs: String): Long =
      (oTs.split('.') match {
        case Array(sec)       => s"${sec}000"
        case Array(sec, msec) => s"${sec}${msec.take(3).padTo(3, '0')}"
      }).toLong

    Try {
      json.transformField {
        case JField("items", jArray) =>
          ("items", jArray.transform {
            case jo: JObject =>
              jo.transformField {
                case JField("timestamp", JString(value)) =>
                  ("timestamp", JString(JsonSchemaDateTimeFormat.print(new DateTime(toMsec(value)))))
              }
          })
      }.successNel
    } match {
      case TF(e) =>
        s"${VendorName} could not convert timestamps: [${JU.stripInstanceEtc(e.getMessage).orNull}]".failureNel
      case TS(s) => s
    }
  }

  /**
   * Converts a querystring payload into an event
   * @param bodyMap The converted map from the querystring
   */
  private def payloadBodyToEvent(bodyMap: Map[String, String]): Validated[JObject] =
    bodyMap.get("data") match {
      case None     => s"${VendorName} event data does not have 'data' as a key".failureNel
      case Some("") => s"${VendorName} event data is empty: nothing to process".failureNel
      case Some(json) => {
        try {
          val event = parse(json)
          event match {
            case obj: JObject => obj.successNel
            case _            => s"${VendorName} event wrong type: [%s]".format(event.getClass).failureNel
          }
        } catch {
          case e: JsonParseException =>
            s"${VendorName} event string failed to parse into JSON: [${JU.stripInstanceEtc(e.getMessage).orNull}]".failureNel
          case NonFatal(e) =>
            s"${VendorName} incorrect event string : [${JU.stripInstanceEtc(e.getMessage).orNull}]".failureNel
        }
      }
    }
}
