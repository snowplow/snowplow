/*
 * Copyright (c) 2015-2019 Snowplow Analytics Ltd. All rights reserved.
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

import javax.mail.internet.ContentType

import scala.util.Try

import cats.Monad
import cats.data.{NonEmptyList, ValidatedNel}
import cats.effect.Clock
import cats.syntax.either._
import cats.syntax.validated._
import com.snowplowanalytics.iglu.client.Client
import com.snowplowanalytics.iglu.client.resolver.registries.RegistryLookup
import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer}
import io.circe.Json
import io.circe.parser._

import loaders.CollectorPayload

/**
 * Transforms a collector payload which conforms to a known version of the Sendgrid Tracking webhook
 * into raw events.
 */
object SendgridAdapter extends Adapter {
  // Vendor name for Failure Message
  private val VendorName = "Sendgrid"

  // Expected content type for a request body
  private val ContentType = "application/json"

  // Tracker version for a Sendgrid Tracking webhook
  private val TrackerVersion = "com.sendgrid-v3"

  private val Vendor = "com.sendgrid"
  private val Format = "jsonschema"
  private val SchemaVersion = SchemaVer.Full(2, 0, 0)

  // Schemas for reverse-engineering a Snowplow unstructured event
  private val EventSchemaMap = Map(
    "processed" -> SchemaKey(Vendor, "processed", Format, SchemaVersion).toSchemaUri,
    "dropped" -> SchemaKey(Vendor, "dropped", Format, SchemaVersion).toSchemaUri,
    "delivered" -> SchemaKey(Vendor, "delivered", Format, SchemaVersion).toSchemaUri,
    "deferred" -> SchemaKey(Vendor, "deferred", Format, SchemaVersion).toSchemaUri,
    "bounce" -> SchemaKey(Vendor, "bounce", Format, SchemaVersion).toSchemaUri,
    "open" -> SchemaKey(Vendor, "open", Format, SchemaVersion).toSchemaUri,
    "click" -> SchemaKey(Vendor, "click", Format, SchemaVersion).toSchemaUri,
    "spamreport" -> SchemaKey(Vendor, "spamreport", Format, SchemaVersion).toSchemaUri,
    "unsubscribe" -> SchemaKey(Vendor, "unsubscribe", Format, SchemaVersion).toSchemaUri,
    "group_unsubscribe" ->
      SchemaKey(Vendor, "group_unsubscribe", Format, SchemaVersion).toSchemaUri,
    "group_resubscribe" -> SchemaKey(Vendor, "group_resubscribe", Format, SchemaVersion).toSchemaUri
  )

  /**
   * Converts a payload into a list of validated events. Expects a valid json - returns a single
   * failure if one is not present
   * @param body json payload as POST'd by sendgrid
   * @param payload the rest of the payload details
   * @return a list of validated events, successes will be the corresponding raw events failures
   * will contain a non empty list of the reason(s) for the particular event failing
   */
  private def payloadBodyToEvents(
    body: String,
    payload: CollectorPayload
  ): List[ValidatedNel[String, RawEvent]] =
    parse(body) match {
      case Right(json) =>
        json.asArray match {
          case Some(array) =>
            array.toList.zipWithIndex.map {
              case (item, index) =>
                val eventType = item.hcursor.downField("event").as[String].toOption
                val queryString = toMap(payload.querystring)
                lookupSchema(eventType, VendorName, index, EventSchemaMap).map { schema =>
                  RawEvent(
                    api = payload.api,
                    parameters = toUnstructEventParams(
                      TrackerVersion,
                      queryString,
                      schema,
                      cleanupJsonEventValues(item, eventType.map(("event", _)), List("timestamp")),
                      "srv"
                    ),
                    contentType = payload.contentType,
                    source = payload.source,
                    context = payload.context
                  )
                }.toValidatedNel
            }
          case None => List(s"$VendorName event is not an array".invalidNel)
        }
      case Left(e) =>
        List(s"$VendorName event failed to parse into JSON: [${e.getMessage}]".invalidNel)
    }

  /**
   * Converts a CollectorPayload instance into raw events. A Sendgrid Tracking payload only contains
   * a single event. We expect the name parameter to be 1 of 6 options otherwise we have an
   * unsupported event type.
   * @param payload The CollectorPayload containing one or more raw events
   * @param client The Iglu client used for schema lookup and validation
   * @return a Validation boxing either a NEL of RawEvents on Success, or a NEL of Failure Strings
   */
  override def toRawEvents[F[_]: Monad: RegistryLookup: Clock](
    payload: CollectorPayload,
    client: Client[F, Json]
  ): F[ValidatedNel[String, NonEmptyList[RawEvent]]] =
    (payload.body, payload.contentType) match {
      case (None, _) =>
        Monad[F].pure(s"Request body is empty: no $VendorName event to process".invalidNel)
      case (_, None) =>
        Monad[F].pure(
          s"Request body provided but content type empty, expected $ContentType for $VendorName".invalidNel
        )
      case (_, Some(ct)) if Try(new ContentType(ct).getBaseType).getOrElse(ct) != ContentType =>
        Monad[F].pure(
          s"Content type of $ct provided, expected $ContentType for $VendorName".invalidNel
        )
      case (Some(body), _) =>
        val _ = client
        val events = payloadBodyToEvents(body, payload)
        Monad[F].pure(rawEventsListProcessor(events))
    }

}
