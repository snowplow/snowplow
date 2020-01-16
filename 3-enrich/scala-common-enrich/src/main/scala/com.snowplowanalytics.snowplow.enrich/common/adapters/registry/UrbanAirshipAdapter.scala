/*
 * Copyright (c) 2015-2020 Snowplow Analytics Ltd. All rights reserved.
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
import cats.syntax.apply._
import cats.syntax.either._
import cats.syntax.option._
import cats.syntax.validated._
import com.snowplowanalytics.iglu.client.Client
import com.snowplowanalytics.iglu.client.resolver.registries.RegistryLookup
import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer}
import com.snowplowanalytics.snowplow.badrows._
import io.circe.{DecodingFailure, Json}
import org.joda.time.{DateTime, DateTimeZone}

import loaders.CollectorPayload
import utils.{HttpClient, JsonUtils}

/**
 * Transforms a collector payload which conforms to a known version of the UrbanAirship Connect API
 * into raw events.
 */
object UrbanAirshipAdapter extends Adapter {
  // Tracker version for an UrbanAirship Connect API
  private val TrackerVersion = "com.urbanairship.connect-v1"

  private val Vendor = "com.urbanairship.connect"
  private val Format = "jsonschema"
  private val SchemaVersion = SchemaVer.Full(1, 0, 0)

  // Schemas for reverse-engineering a Snowplow unstructured event
  private val EventSchemaMap = Map(
    "CLOSE" -> SchemaKey(Vendor, "CLOSE", Format, SchemaVersion),
    "CUSTOM" -> SchemaKey(Vendor, "CUSTOM", Format, SchemaVersion),
    "FIRST_OPEN" -> SchemaKey(Vendor, "FIRST_OPEN", Format, SchemaVersion),
    "IN_APP_MESSAGE_DISPLAY" ->
      SchemaKey(Vendor, "IN_APP_MESSAGE_DISPLAY", Format, SchemaVersion),
    "IN_APP_MESSAGE_EXPIRATION" ->
      SchemaKey(Vendor, "IN_APP_MESSAGE_EXPIRATION", Format, SchemaVersion),
    "IN_APP_MESSAGE_RESOLUTION" ->
      SchemaKey(Vendor, "IN_APP_MESSAGE_RESOLUTION", Format, SchemaVersion),
    "LOCATION" -> SchemaKey(Vendor, "LOCATION", Format, SchemaVersion),
    "OPEN" -> SchemaKey(Vendor, "OPEN", Format, SchemaVersion),
    "PUSH_BODY" -> SchemaKey(Vendor, "PUSH_BODY", Format, SchemaVersion),
    "REGION" -> SchemaKey(Vendor, "REGION", Format, SchemaVersion),
    "RICH_DELETE" -> SchemaKey(Vendor, "RICH_DELETE", Format, SchemaVersion),
    "RICH_DELIVERY" -> SchemaKey(Vendor, "RICH_DELIVERY", Format, SchemaVersion),
    "RICH_HEAD" -> SchemaKey(Vendor, "RICH_HEAD", Format, SchemaVersion),
    "SEND" -> SchemaKey(Vendor, "SEND", Format, SchemaVersion),
    "TAG_CHANGE" -> SchemaKey(Vendor, "TAG_CHANGE", Format, SchemaVersion),
    "UNINSTALL" -> SchemaKey(Vendor, "UNINSTALL", Format, SchemaVersion)
  )

  /**
   * Converts a CollectorPayload instance into raw events. A UrbanAirship connect API payload only
   * contains a single event. We expect the name parameter to match the supported events, else
   * we have an unsupported event type.
   * @param payload The CollectorPayload containing one or more raw events
   * @param client The Iglu client used for schema lookup and validation
   * @return a Validation boxing either a NEL of RawEvents on Success, or a NEL of Failure Strings
   */
  override def toRawEvents[F[_]: Monad: RegistryLookup: Clock: HttpClient](
    payload: CollectorPayload,
    client: Client[F, Json]
  ): F[
    ValidatedNel[FailureDetails.AdapterFailureOrTrackerProtocolViolation, NonEmptyList[RawEvent]]
  ] =
    (payload.body, payload.contentType) match {
      case (None, _) =>
        val failure = FailureDetails.AdapterFailure.InputData(
          "body",
          none,
          "empty body: no events to process"
        )
        Monad[F].pure(failure.invalidNel)
      case (_, Some(ct)) =>
        val msg = s"expected no content type"
        Monad[F].pure(
          FailureDetails.AdapterFailure
            .InputData("contentType", ct.some, msg)
            .invalidNel
        )
      case (Some(body), _) =>
        val _ = client
        val event = payloadBodyToEvent(body, payload)
        Monad[F].pure(rawEventsListProcessor(List(event)))
    }

  /**
   * Converts payload into a single validated event. Expects a valid json, returns failure if one is
   * not present.
   * @param bodyJson json payload as a string
   * @param payload other payload details
   * @return a validated event - a success is the RawEvent, failures will contain the reasons
   */
  private def payloadBodyToEvent(
    bodyJson: String,
    payload: CollectorPayload
  ): ValidatedNel[FailureDetails.AdapterFailure, RawEvent] = {
    def toTtmFormat(jsonTimestamp: String) =
      "%d".format(new DateTime(jsonTimestamp).getMillis)

    def err(field: String, e: DecodingFailure): FailureDetails.AdapterFailure.InputData =
      FailureDetails.AdapterFailure.InputData(
        field,
        none,
        s"could not extract '$field': ${e.getMessage}"
      )

    JsonUtils.extractJson(bodyJson) match {
      case Right(json) =>
        val cursor = json.hcursor
        val eventType = cursor.get[String]("type").toOption
        val trueTs = cursor.get[String]("occurred").leftMap(err("occurred", _)).toValidatedNel
        val eid = cursor.get[String]("id").leftMap(err("id", _)).toValidatedNel
        val collectorTs = cursor
          .get[String]("processed")
          .leftMap(err("processed", _))
          .toValidatedNel
        (
          trueTs,
          eid,
          collectorTs,
          lookupSchema(eventType, EventSchemaMap).toValidatedNel
        ).mapN { (tts, id, cts, schema) =>
          RawEvent(
            api = payload.api,
            parameters = toUnstructEventParams(
              TrackerVersion,
              toMap(payload.querystring) ++ Map("ttm" -> toTtmFormat(tts), "eid" -> id),
              schema,
              json,
              "srv"
            ),
            contentType = payload.contentType,
            source = payload.source,
            context = payload.context.copy(timestamp = Some(new DateTime(cts, DateTimeZone.UTC)))
          )
        }
      case Left(e) =>
        FailureDetails.AdapterFailure.NotJson("body", bodyJson.some, e).invalidNel
    }
  }
}
