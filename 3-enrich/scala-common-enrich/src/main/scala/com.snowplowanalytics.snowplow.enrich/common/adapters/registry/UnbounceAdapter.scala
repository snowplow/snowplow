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

import scala.util.{Try, Success => TS, Failure => TF}
import scala.collection.JavaConversions._

import cats.data.{NonEmptyList, ValidatedNel}
import cats.syntax.apply._
import cats.syntax.either._
import cats.syntax.validated._
import com.snowplowanalytics.iglu.client.{Resolver, SchemaKey}
import io.circe._
import io.circe.parser._
import org.apache.http.client.utils.URLEncodedUtils

import loaders.CollectorPayload
import utils.{JsonUtils => JU}

/**
 * Transforms a collector payload which conforms to a known version of the Unbounce Tracking webhook
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
   * Converts a CollectorPayload instance into raw events. An Unbounce Tracking payload contains one
   * single event in the body of the payload, stored within a HTTP encoded string.
   * @param payload The CollectorPayload containing one or more raw events
   * @param resolver (implicit) The Iglu resolver used for schema lookup and validation. Not used
   * @return a Validation boxing either a NEL of RawEvents on Success, or a NEL of Failure Strings
   */
  def toRawEvents(payload: CollectorPayload)(
    implicit resolver: Resolver
  ): ValidatedNel[String, NonEmptyList[RawEvent]] =
    (payload.body, payload.contentType) match {
      case (None, _) => s"Request body is empty: no $VendorName events to process".invalidNel
      case (_, None) =>
        s"Request body provided but content type empty, expected $ContentType for $VendorName".invalidNel
      case (_, Some(ct)) if ct != ContentType =>
        s"Content type of $ct provided, expected $ContentType for $VendorName".invalidNel
      case (Some(body), _) =>
        if (body.isEmpty) s"$VendorName event body is empty: nothing to process".invalidNel
        else {
          val qsParams = toMap(payload.querystring)
          Try {
            toMap(URLEncodedUtils.parse(URI.create("http://localhost/?" + body), UTF_8).toList)
          } match {
            case TF(e) =>
              val msg = JU.stripInstanceEtc(e.getMessage).orNull
              s"$VendorName incorrect event string : [$msg]".invalidNel
            case TS(bodyMap) =>
              (
                payloadBodyToEvent(bodyMap).toValidatedNel,
                lookupSchema(Some("form_post"), VendorName, ContextSchema).toValidatedNel
              ).mapN { (event, schema) =>
                NonEmptyList.one(
                  RawEvent(
                    api = payload.api,
                    parameters =
                      toUnstructEventParams(TrackerVersion, qsParams, schema, event, "srv"),
                    contentType = payload.contentType,
                    source = payload.source,
                    context = payload.context
                  )
                )
              }
          }
        }
    }

  private def payloadBodyToEvent(bodyMap: Map[String, String]): Either[String, Json] =
    (
      bodyMap.get("page_id"),
      bodyMap.get("page_name"),
      bodyMap.get("variant"),
      bodyMap.get("page_url"),
      bodyMap.get("data.json")
    ) match {
      case (None, _, _, _, _) => s"${VendorName} context data missing 'page_id'".asLeft
      case (_, None, _, _, _) => s"${VendorName} context data missing 'page_name'".asLeft
      case (_, _, None, _, _) => s"${VendorName} context data missing 'variant'".asLeft
      case (_, _, _, None, _) => s"${VendorName} context data missing 'page_url'".asLeft
      case (_, _, _, _, None) =>
        s"$VendorName event data does not have 'data.json' as a key".asLeft
      case (_, _, _, _, Some(dataJson)) if dataJson.isEmpty =>
        s"$VendorName event data is empty: nothing to process".asLeft
      case (Some(pageId), Some(pageName), Some(variant), Some(pageUrl), Some(dataJson)) =>
        val event = (bodyMap - "data.json" - "data.xml").toList
        parse(dataJson)
          .map { dJs =>
            val js = Json
              .obj(
                ("data.json", dJs) :: event.map { case (k, v) => (k, Json.fromString(v)) }: _*
              )
            camelize(js)
          }
          .leftMap(e => s"$VendorName event string failed to parse into JSON: [${e.getMessage}]")
    }
}
