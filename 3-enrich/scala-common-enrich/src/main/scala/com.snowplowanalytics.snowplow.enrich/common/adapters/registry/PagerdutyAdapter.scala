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
import utils.JsonUtils

/**
 * Transforms a collector payload which conforms to a known version of the PagerDuty Tracking
 * webhook into raw events.
 */
object PagerdutyAdapter extends Adapter {
  // Tracker version for a PagerDuty webhook
  private val TrackerVersion = "com.pagerduty-v1"

  // Expected content type for a request body
  private val ContentType = "application/json"

  // Event-Schema Map for reverse-engineering a Snowplow unstructured event
  private val Incident =
    SchemaKey("com.pagerduty", "incident", "jsonschema", SchemaVer.Full(1, 0, 0)).toSchemaUri
  private[registry] val EventSchemaMap = Map(
    "incident.trigger" -> Incident,
    "incident.acknowledge" -> Incident,
    "incident.unacknowledge" -> Incident,
    "incident.resolve" -> Incident,
    "incident.assign" -> Incident,
    "incident.escalate" -> Incident,
    "incident.delegate" -> Incident
  )

  /**
   * Converts a CollectorPayload instance into raw events. A PagerDuty Tracking payload can contain
   * many events in one. We expect the type parameter to be 1 of 7 options otherwise we have an
   * unsupported event type.
   * @param payload The CollectorPaylod containing one or more raw events
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
                val eventOpt = event.hcursor.downField("type").as[String].toOption
                for {
                  schema <- lookupSchema(eventOpt, index, EventSchemaMap).toValidatedNel
                } yield {
                  val formattedEvent = reformatParameters(event)
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
   * Returns a list of JValue events from the PagerDuty payload
   * @param body The payload body from the PagerDuty event
   * @return either a Successful List of JValue JSONs or a Failure String
   */
  private[registry] def payloadBodyToEvents(body: String): Either[AdapterFailure, List[Json]] =
    JsonUtils
      .extractJson(body)
      .leftMap(e => NotJsonAdapterFailure("body", body.some, e))
      .flatMap { p =>
        p.hcursor.downField("messages").focus.flatMap(_.asArray) match {
          case Some(array) => array.toList.asRight
          case None =>
            InputDataAdapterFailure("messages", body.some, "field `messages` is not an array").asLeft
        }
      }

  /**
   * Returns an updated date-time string for cases where PagerDuty does not pass a '+' or '-' with
   * the date-time.
   * e.g. "2014-11-12T18:53:47 00:00" ->
   *      "2014-11-12T18:53:47+00:00"
   * @param dt The date-time we need to potentially reformat
   * @return the date-time which is now correctly formatted
   */
  private[registry] def formatDatetime(dt: String): String =
    dt.replaceAll(" 00:00$", "+00:00")

  /**
   * Returns an updated event JSON where all of the fields with a null string have been changed to a
   * null value, all event types have been trimmed and all timestamps have been correctly formatted.
   * e.g. "event" -> "null"
   *      "event" -> null
   * e.g. "type" -> "incident.trigger"
   *      "type" -> "trigger"
   * @param json The event JSON which we need to update values within
   * @return the updated JSON with valid null values, type values and formatted date-time strings
   */
  private[registry] def reformatParameters(json: Json): Json =
    json.mapObject { obj =>
      val updatedObj = obj.toMap.map {
        case (k, v) if v == Json.fromString("null") => (k, Json.Null)
        case ("type", v) if v.isString => ("type", v.mapString(_.replace("incident.", "")))
        case ("created_on", v) if v.isString => ("created_on", v.mapString(formatDatetime))
        case ("last_status_change_on", v) if v.isString =>
          ("last_status_change_on", v.mapString(formatDatetime))
        case (k, v) if v.isObject => (k, reformatParameters(v))
        case (k, v) => (k, v)
      }
      JsonObject(updatedObj.toList: _*)
    }
}
