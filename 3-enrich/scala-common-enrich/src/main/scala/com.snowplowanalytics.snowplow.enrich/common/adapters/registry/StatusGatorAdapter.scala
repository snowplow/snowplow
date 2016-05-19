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
 * a known version of the StatusGator Tracking webhook
 * into raw events.
 */
object StatusGatorAdapter extends Adapter {

  // Vendor name for Failure Message
  private val VendorName = "StatusGator"

  // Tracker version for an StatusGator Tracking webhook
  private val TrackerVersion = "com.statusgator-v1"

  // Expected content type for a request body
  private val ContentType = "application/x-www-form-urlencoded"

  // Schemas for reverse-engineering a Snowplow unstructured event
  private val EventSchemaMap = Map (
    "status_change" -> SchemaKey("com.statusgator", "status_change", "jsonschema", "1-0-0").toSchemaUri
  )

  /**
   * Converts a CollectorPayload instance into raw events.
   *
   * A StatusGator Tracking payload contains one single event
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
      case (None, _)                          => s"Request body is empty: no ${VendorName} events to process".failNel
      case (_, None)                          => s"Request body provided but content type empty, expected ${ContentType} for ${VendorName}".failNel
      case (_, Some(ct)) if ct != ContentType => s"Content type of ${ct} provided, expected ${ContentType} for ${VendorName}".failNel
      case (Some(body),_)                     => {
        if (body.isEmpty) {
          s"${VendorName} event body is empty: nothing to process".failNel
        } else {
          val qsParams = toMap(payload.querystring)
          try {
            val bodyMap = toMap(URLEncodedUtils.parse(URI.create("http://localhost/?" + body), "UTF-8").toList)
            val json = compact(render(bodyMap))
            val event = parse(json)
            lookupSchema(Some("status_change"), VendorName, EventSchemaMap) match {
              case Failure(str)  => str.fail
              case Success(schema) => {
                NonEmptyList(RawEvent(
                  api          = payload.api,
                  parameters   = toUnstructEventParams(TrackerVersion, qsParams, schema, event, "srv"),
                  contentType  = payload.contentType,
                  source       = payload.source,
                  context      = payload.context
                )).success
              }
            }
          } catch {
            case e: JsonParseException => {
              val exception = JU.stripInstanceEtc(e.toString).orNull
              s"${VendorName} event string failed to parse into JSON: [${exception}]".failNel
            }
            case e: Exception => {
              val exception = JU.stripInstanceEtc(e.toString).orNull
              s"${VendorName} incorrect event string : [${exception}]".failNel
            }
          }
        }
      }
    }
}
