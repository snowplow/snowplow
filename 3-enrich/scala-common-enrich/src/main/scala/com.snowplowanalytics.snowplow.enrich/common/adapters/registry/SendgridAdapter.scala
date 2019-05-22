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

import cats.Monad
import cats.data.{NonEmptyList, ValidatedNel}
import cats.effect.Clock
import cats.syntax.either._
import cats.syntax.option._
import cats.syntax.validated._
import com.snowplowanalytics.iglu.client.Client
import com.snowplowanalytics.iglu.client.resolver.registries.RegistryLookup
import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer}
import io.circe.Json

import loaders.CollectorPayload
import outputs._
import utils.JsonUtils

/**
 * Transforms a collector payload which conforms to a known version of the Sendgrid Tracking webhook
 * into raw events.
 */
object SendgridAdapter extends Adapter {
  // Expected content type for a request body
  private val ContentType = "application/json"

  // Tracker version for a Sendgrid Tracking webhook
  private val TrackerVersion = "com.sendgrid-v3"

  private val Vendor = "com.sendgrid"
  private val Format = "jsonschema"
  private val SchemaVersion = SchemaVer.Full(2, 0, 0)

  // Schemas for reverse-engineering a Snowplow unstructured event
  private[registry] val EventSchemaMap = Map(
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
  ): F[ValidatedNel[AdapterFailure, NonEmptyList[RawEvent]]] =
    (payload.body, payload.contentType) match {
      case (None, _) =>
        val failure = InputDataAdapterFailure("body", none, "empty body: no events to process")
        Monad[F].pure(failure.invalidNel)
      case (_, None) =>
        val msg = s"no content type: expected $ContentType"
        Monad[F].pure(InputDataAdapterFailure("contentType", none, msg).invalidNel)
      case (_, Some(ct)) if !ct.contains(ContentType) =>
        val msg = s"expected $ContentType"
        Monad[F].pure(InputDataAdapterFailure("contentType", ct.some, msg).invalidNel)
      case (Some(body), _) =>
        val _ = client
        val events = payloadBodyToEvents(body, payload)
        Monad[F].pure(rawEventsListProcessor(events))
    }

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
  ): List[ValidatedNel[AdapterFailure, RawEvent]] =
    JsonUtils.extractJson(body) match {
      case Right(json) =>
        json.asArray match {
          case Some(array) =>
            array.toList.zipWithIndex.map {
              case (item, index) =>
                val eventType = item.hcursor.downField("event").as[String].toOption
                val queryString = toMap(payload.querystring)
                lookupSchema(eventType, index, EventSchemaMap).map { schema =>
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
          case None =>
            List(
              InputDataAdapterFailure("body", body.some, "body is not a json array").invalidNel
            )
        }
      case Left(e) => List(NotJsonAdapterFailure("body", body, e).invalidNel)
    }

}
