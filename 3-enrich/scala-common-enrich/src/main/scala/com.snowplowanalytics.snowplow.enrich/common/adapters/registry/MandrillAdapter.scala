/*
 * Copyright (c) 2014-2019 Snowplow Analytics Ltd. All rights reserved.
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
import com.snowplowanalytics.snowplow.badrows.AdapterFailure
import com.snowplowanalytics.snowplow.badrows.AdapterFailure._
import io.circe._

import loaders.CollectorPayload
import utils.{HttpClient, JsonUtils}
import com.snowplowanalytics.snowplow.enrich.common.utils.ConversionUtils

/**
 * Transforms a collector payload which conforms to a known version of the Mandrill Tracking webhook
 * into raw events.
 */
object MandrillAdapter extends Adapter {
  // Tracker version for an Mandrill Tracking webhook
  private val TrackerVersion = "com.mandrill-v1"

  // Expected content type for a request body
  private val ContentType = "application/x-www-form-urlencoded"

  private val Vendor = "com.mandrill"
  private val Format = "jsonschema"
  private val SchemaVersion = SchemaVer.Full(1, 0, 1)

  // Schemas for reverse-engineering a Snowplow unstructured event
  private[registry] val EventSchemaMap = Map(
    "hard_bounce" -> SchemaKey(Vendor, "message_bounced", Format, SchemaVersion).toSchemaUri,
    "click" -> SchemaKey(Vendor, "message_clicked", Format, SchemaVersion).toSchemaUri,
    "deferral" -> SchemaKey(Vendor, "message_delayed", Format, SchemaVersion).toSchemaUri,
    "spam" -> SchemaKey(Vendor, "message_marked_as_spam", Format, SchemaVersion).toSchemaUri,
    "open" -> SchemaKey(Vendor, "message_opened", Format, SchemaVersion).toSchemaUri,
    "reject" -> SchemaKey(Vendor, "message_rejected", Format, SchemaVer.Full(1, 0, 0)).toSchemaUri,
    "send" -> SchemaKey(Vendor, "message_sent", Format, SchemaVer.Full(1, 0, 0)).toSchemaUri,
    "soft_bounce" -> SchemaKey(Vendor, "message_soft_bounced", Format, SchemaVersion).toSchemaUri,
    "unsub" -> SchemaKey(Vendor, "recipient_unsubscribed", Format, SchemaVersion).toSchemaUri
  )

  /**
   * Converts a CollectorPayload instance into raw events.
   * A Mandrill Tracking payload contains many events in the body of the payload, stored within a
   * HTTP encoded string.
   * We expect the event parameter of these events to be 1 of 9 options otherwise we have an
   * unsupported event type.
   * @param payload The CollectorPayload containing one or more raw events as collected by a
   * Snowplow collector
   * @param client The Iglu client used for schema lookup and validation
   * @return a Validation boxing either a NEL of RawEvents on Success, or a NEL of Failure Strings
   */
  override def toRawEvents[F[_]: Monad: RegistryLookup: Clock: HttpClient](
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
      case (_, Some(ct)) if ct != ContentType =>
        val msg = s"expected $ContentType"
        Monad[F].pure(InputDataAdapterFailure("contentType", ct.some, msg).invalidNel)
      case (Some(body), _) =>
        payloadBodyToEvents(body) match {
          case Left(str) => Monad[F].pure(str.invalidNel)
          case Right(list) =>
            val _ = client
            // Create our list of Validated RawEvents
            val rawEventsList: List[ValidatedNel[AdapterFailure, RawEvent]] =
              for {
                (event, index) <- list.zipWithIndex
              } yield {
                val eventOpt = event.hcursor.get[String]("event").toOption
                for {
                  schema <- lookupSchema(eventOpt, index, EventSchemaMap).toValidatedNel
                } yield {
                  val formattedEvent =
                    cleanupJsonEventValues(event, eventOpt.map(("event", _)), List("ts"))
                  val qsParams = toMap(payload.querystring)
                  RawEvent(
                    api = payload.api,
                    parameters = toUnstructEventParams(
                      TrackerVersion,
                      qsParams,
                      schema,
                      formattedEvent,
                      "srv"
                    ),
                    contentType = payload.contentType,
                    source = payload.source,
                    context = payload.context
                  )
                }
              }
            // Processes the List for Failures and Successes and returns ValidatedRawEvents
            Monad[F].pure(rawEventsListProcessor(rawEventsList))
        }
    }

  /**
   * Returns a list of events from the payload body of a Mandrill Event. Each event will be
   * formatted as an individual JSON.
   * NOTE: The payload.body string must adhere to UTF-8 encoding standards.
   * @param rawEventString The encoded string from the Mandrill payload body
   * @return a list of single events formatted as JSONs or a Failure String
   */
  private[registry] def payloadBodyToEvents(
    rawEventString: String
  ): Either[AdapterFailure, List[Json]] =
    for {
      bodyMap <- ConversionUtils
        .parseUrlEncodedForm(rawEventString)
        .leftMap(e => InputDataAdapterFailure("body", rawEventString.some, e))
      res <- bodyMap match {
        case map if map.size != 1 =>
          val msg = s"body should have size 1: actual size ${map.size}"
          InputDataAdapterFailure("body", rawEventString.some, msg).asLeft
        case map =>
          map.get("mandrill_events") match {
            case None =>
              val msg = "no `mandrill_events` parameter provided"
              InputDataAdapterFailure("body", rawEventString.some, msg).asLeft
            case Some("") =>
              val msg = "`mandrill_events` field is empty"
              InputDataAdapterFailure("body", rawEventString.some, msg).asLeft
            case Some(dStr) =>
              JsonUtils
                .extractJson(dStr)
                .leftMap(e => NotJsonAdapterFailure("mandril_events", dStr.some, e))
                .flatMap { json =>
                  json.asArray match {
                    case Some(array) => array.toList.asRight
                    case _ =>
                      val msg = "not a json array"
                      InputDataAdapterFailure("mandrill_events", dStr.some, msg).asLeft
                  }
                }
          }
      }
    } yield res

}
