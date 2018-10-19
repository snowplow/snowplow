/*
 * Copyright (c) 2014-2018 Snowplow Analytics Ltd. All rights reserved.
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

// Iglu
import iglu.client.{Resolver, SchemaKey}

// Scala
import scala.collection.JavaConversions._

// Scalaz
import scalaz._
import Scalaz._

// json4s
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import com.fasterxml.jackson.core.JsonParseException

// This project
import loaders.CollectorPayload
import loaders.CollectorPayload._
import utils.{JsonUtils => JU}

import com.snowplowanalytics.iglu.core.SelfDescribingData
import com.snowplowanalytics.iglu.core.circe.implicits._

/**
 * Transforms a collector payload which either:
 * 1. Provides a set of name-value pairs on a GET querystring
 *    with a &schema=[[iglu schema uri]] parameter.
 * 2. Provides a &schema=[[iglu schema uri]] parameter on a POST
 *    querystring and a set of name-value pairs in the body.
 *    - Formatted as JSON
 *    - Formatted as a Form Body
 */
object IgluAdapter extends Adapter {

  // Vendor name for Failure Message
  private val VendorName = "Iglu"

  // Tracker version for an Iglu-compatible webhook
  private val TrackerVersion = "com.snowplowanalytics.iglu-v1"

  // Create a simple formatter function
  private val IgluFormatter: FormatterFunc = buildFormatter() // For defaults

  /**
   * Converts a CollectorPayload instance into raw events.
   * Currently we only support a single event Iglu-compatible
   * self-describing event passed in on the querystring.
   *
   * @param payload The CollectorPayload containing one or more
   *        raw events as collected by a Snowplow collector
   * @param resolver (implicit) The Iglu resolver used for
   *        schema lookup and validation. Not used
   * @return a Validation boxing either a NEL of RawEvents on
   *         Success, or a NEL of Failure Strings
   */
  def toRawEvents(payload: CollectorPayload)(implicit resolver: Resolver): ValidatedRawEvents =
    payload match {
      case p: TrackerPayload => // Technically it is a webhook
        val params = toMap(payload.querystring)
        (params.get("schema"), p.body, payload.contentType) match {
          case (_, Some(_), None)                    => s"$VendorName event failed: ContentType must be set for a POST payload".failNel
          case (None, Some(body), Some(contentType)) => payloadSdJsonToEvent(payload, body, contentType, params)
          case (Some(schemaUri), Some(_), Some(_))   => payloadToEventWithSchema(p, schemaUri, params)
          case (Some(schemaUri), None, _)            => payloadToEventWithSchema(p, schemaUri, params)
          case (_, _, _)                             => s"$VendorName event failed: is not a sd-json or a valid GET or POST request".failNel
        }

    }

  // --- SelfDescribingJson Payloads

  /**
   * Processes a potential SelfDescribingJson into a
   * validated raw-event.
   *
   * @param payload The CollectorPaylod containing one or more
   *        raw events as collected by a Snowplow collector
   * @param body The extracted body string
   * @param contentType The extracted contentType string
   * @param params The raw map of params from the querystring.
   */
  private[registry] def payloadSdJsonToEvent(payload: CollectorPayload,
                                             body: io.circe.Json,
                                             contentType: String,
                                             params: Map[String, String]): ValidatedRawEvents =
    contentType match {
      case "application/json"                => sdJsonBodyToEvent(payload, body, params)
      case "application/json; charset=utf-8" => sdJsonBodyToEvent(payload, body, params)
      case _                                 => "Content type not supported".failNel
    }

  /**
   * Processes a potential SelfDescribingJson into a
   * validated raw-event.
   *
   * @param payload The CollectorPayload containing one or more
   *        raw events as collected by a Snowplow collector
   * @param body The extracted body string
   * @param params The raw map of params from the querystring.
   */
  private[registry] def sdJsonBodyToEvent(payload: CollectorPayload,
                                          body: io.circe.Json,
                                          params: Map[String, String]): ValidatedRawEvents =
    body.toData match {
      case Some(SelfDescribingData(schema, data)) =>
        NonEmptyList(
          RawEvent(
            api         = payload.api,
            parameters  = toUnstructEventParams(TrackerVersion, params, schema.toSchemaUri, data, "app"),
            contentType = payload.contentType,
            source      = payload.source,
            context     = payload.context
          )).success
      case None => s"$VendorName event failed: detected SelfDescribingJson but schema key is missing".failNel
    }

  // --- Payloads with the Schema in the Query-String

  /**
   * Processes a payload that has the schema field in
   * the query-string.
   *
   * @param payload The CollectorPaylod containing one or more
   *        raw events as collected by a Snowplow collector
   * @param schemaUri The schema-uri found
   * @param params The raw map of params from the querystring.
   */
  private[registry] def payloadToEventWithSchema(payload: TrackerPayload,
                                                 schemaUri: String,
                                                 params: Map[String, String]): ValidatedRawEvents =
    SchemaKey.parse(schemaUri) match {
      case Failure(procMsg) => procMsg.getMessage.failNel
      case Success(_) =>
        (payload.body, payload.contentType) match {
          case (None, _) =>
            NonEmptyList(
              RawEvent(
                api         = payload.api,
                parameters  = toUnstructEventParams(TrackerVersion, params - "schema", schemaUri, IgluFormatter, "app"),
                contentType = payload.contentType,
                source      = payload.source,
                context     = payload.context
              )).success
          case (Some(body), Some(contentType)) =>
            contentType match {
              case "application/json"                  => jsonBodyToEvent(payload, body, schemaUri, params)
              case "application/json; charset=utf-8"   => jsonBodyToEvent(payload, body, schemaUri, params)
              case "application/x-www-form-urlencoded" => formBodyToEvent(payload, body, schemaUri, params)
              case _                                   => "Content type not supported".failNel
            }
          case (_, None) => "Content type has not been specified".failNel
        }
    }

  /**
   * Converts a json payload into a single validated event
   *
   * @param body json payload as POST'd by a webhook
   * @param payload the rest of the payload details
   * @param schemaUri the schemaUri for the event
   * @param params The query string parameters
   * @return a single validated event
   */
  private[registry] def jsonBodyToEvent(payload: TrackerPayload,
                                        body: io.circe.Json,
                                        schemaUri: String,
                                        params: Map[String, String]): ValidatedRawEvents = {
    def buildRawEvent(e: io.circe.Json): RawEvent =
      RawEvent(
        api         = payload.api,
        parameters  = toUnstructEventParams(TrackerVersion, params - "schema", schemaUri, e, "app"),
        contentType = payload.contentType,
        source      = payload.source,
        context     = payload.context
      )

    body.asArray match {
      case Some(v) if v.isEmpty =>
        s"$VendorName event failed json sanity check: array of events cannot be empty".failNel
      case Some(array) =>
        (NonEmptyList(buildRawEvent(array.head)) :::> array.tail.toList.map(buildRawEvent)).success
    }
  }

  /**
   * Converts a form body payload into a single validated event
   *
   * @param body the form body from the payload as
   *        POST'd by a webhook
   * @param payload the rest of the payload details
   * @param schemaUri the schemaUri for the event
   * @param params The query string parameters
   * @return a single validated event
   */
  private[registry] def formBodyToEvent(payload: TrackerPayload,
                                        body: io.circe.Json,
                                        schemaUri: String,
                                        params: Map[String, String]): ValidatedRawEvents =
    try {
      val bodyMap = toMap(URLEncodedUtils.parse(URI.create("http://localhost/?" + body), "UTF-8").toList)
      val json    = compact(render(bodyMap))
      val event   = parse(json)

      NonEmptyList(
        RawEvent(
          api         = payload.api,
          parameters  = toUnstructEventParams(TrackerVersion, params - "schema", schemaUri, event, "srv"),
          contentType = payload.contentType,
          source      = payload.source,
          context     = payload.context
        )).success
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
