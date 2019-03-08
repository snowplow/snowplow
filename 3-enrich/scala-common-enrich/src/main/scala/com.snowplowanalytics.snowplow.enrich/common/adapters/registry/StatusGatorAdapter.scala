/*
 * Copyright (c) 2016-2019 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.enrich.common
package adapters
package registry

import java.net.URI
import java.nio.charset.StandardCharsets.UTF_8

import scala.collection.JavaConversions._
import scala.util.{Try, Success => TS, Failure => TF}

import com.fasterxml.jackson.core.JsonParseException
import com.snowplowanalytics.iglu.client.{Resolver, SchemaKey}
import org.apache.http.client.utils.URLEncodedUtils
import scalaz._
import Scalaz._
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

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
  private val EventSchema = SchemaKey("com.statusgator", "status_change", "jsonschema", "1-0-0").toSchemaUri

  /**
   * Converts a CollectorPayload instance into raw events.
   *
   * A StatusGator Tracking payload contains one single event
   * in the body of the payload, stored within a HTTP encoded
   * string.
   *
   * @param payload  The CollectorPayload containing one or more
   *                 raw events as collected by a Snowplow collector
   * @param resolver (implicit) The Iglu resolver used for
   *                 schema lookup and validation. Not used
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
        Try {
          toMap(URLEncodedUtils.parse(URI.create("http://localhost/?" + body), UTF_8).toList)
        } match {
          case TF(e) =>
            s"${VendorName} incorrect event string : [${JU.stripInstanceEtc(e.getMessage).orNull}]".failureNel
          case TS(bodyMap) =>
            try {
              val a: Map[String, String] = bodyMap
              val event = parse(compact(render(a)))
              NonEmptyList(
                RawEvent(
                  api = payload.api,
                  parameters = toUnstructEventParams(TrackerVersion, qsParams, EventSchema, camelize(event), "srv"),
                  contentType = payload.contentType,
                  source = payload.source,
                  context = payload.context
                )).success
            } catch {
              case e: JsonParseException =>
                s"${VendorName} event string failed to parse into JSON: [${JU.stripInstanceEtc(e.getMessage).orNull}]".failureNel
            }
        }
      }
    }
}
