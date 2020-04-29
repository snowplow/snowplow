/*
 * Copyright (c) 2014-2020 Snowplow Analytics Ltd. All rights reserved.
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
import cats.data.{Kleisli, NonEmptyList, ValidatedNel}
import cats.effect.Clock
import cats.instances.option._
import cats.syntax.either._
import cats.syntax.option._
import cats.syntax.validated._
import com.snowplowanalytics.iglu.client.Client
import com.snowplowanalytics.iglu.client.resolver.registries.RegistryLookup
import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer}
import com.snowplowanalytics.snowplow.badrows._
import io.circe._
import org.joda.time.DateTime

import loaders.CollectorPayload
import utils.{HttpClient, JsonUtils}

/**
 * Transforms a collector payload which conforms to a known version of the HubSpot webhook
 * subscription into raw events.
 */
object HubSpotAdapter extends Adapter {
  // Tracker version for a HubSpot webhook
  private val TrackerVersion = "com.hubspot-v1"

  // Expected content type for a request body
  private val ContentType = "application/json"

  private val Vendor = "com.hubspot"
  private val Format = "jsonschema"
  private val SchemaVersion = SchemaVer.Full(1, 0, 0)

  // Event-Schema Map for reverse-engineering a Snowplow unstructured event
  private[registry] val EventSchemaMap = Map(
    "contact.creation" ->
      SchemaKey(Vendor, "contact_creation", Format, SchemaVersion),
    "contact.deletion" ->
      SchemaKey(Vendor, "contact_deletion", Format, SchemaVersion),
    "contact.propertyChange" ->
      SchemaKey(Vendor, "contact_change", Format, SchemaVersion),
    "company.creation" ->
      SchemaKey(Vendor, "company_creation", Format, SchemaVersion),
    "company.deletion" ->
      SchemaKey(Vendor, "company_deletion", Format, SchemaVersion),
    "company.propertyChange" ->
      SchemaKey(Vendor, "company_change", Format, SchemaVersion),
    "deal.creation" ->
      SchemaKey(Vendor, "deal_creation", Format, SchemaVersion),
    "deal.deletion" ->
      SchemaKey(Vendor, "deal_deletion", Format, SchemaVersion),
    "deal.propertyChange" ->
      SchemaKey(Vendor, "deal_change", Format, SchemaVersion)
  )

  /**
   * Converts a CollectorPayload instance into raw events. A HubSpot Tracking payload can contain
   * many events in one. We expect the type parameter to be 1 of 9 options otherwise we have an
   * unsupported event type.
   * @param payload CollectorPayload containing one or more raw events
   * @param client The Iglu client used for schema lookup and validation
   * @return a Validation boxing either a NEL of RawEvents on Success, or a NEL of Failure Strings
   */
  override def toRawEvents[F[_]: Monad: RegistryLookup: Clock: HttpClient](payload: CollectorPayload, client: Client[F, Json]): F[
    ValidatedNel[FailureDetails.AdapterFailureOrTrackerProtocolViolation, NonEmptyList[RawEvent]]
  ] =
    (payload.body, payload.contentType) match {
      case (None, _) =>
        val failure = FailureDetails.AdapterFailure.InputData(
          "body",
          none,
          "empty body: not events to process"
        )
        Monad[F].pure(failure.invalidNel)
      case (_, None) =>
        val msg = s"no content type: expected $ContentType"
        Monad[F].pure(
          FailureDetails.AdapterFailure.InputData("contentType", none, msg).invalidNel
        )
      case (_, Some(ct)) if ct != ContentType =>
        val failure = FailureDetails.AdapterFailure.InputData(
          "contentType",
          ct.some,
          s"expected $ContentType"
        )
        Monad[F].pure(failure.invalidNel)
      case (Some(body), _) =>
        payloadBodyToEvents(body) match {
          case Left(f) => Monad[F].pure(f.invalidNel)
          case Right(list) =>
            val _ = client
            // Create our list of Validated RawEvents
            val rawEventsList: List[ValidatedNel[FailureDetails.AdapterFailure, RawEvent]] =
              for {
                (event, index) <- list.zipWithIndex
              } yield {
                val eventType = event.hcursor.get[String]("subscriptionType").toOption
                for {
                  schema <- lookupSchema(eventType, index, EventSchemaMap).toValidatedNel
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
   * Returns a list of JValue events from the HubSpot payload
   * @param body The payload body from the HubSpot event
   * @return either a Successful List of JValue JSONs or a Failure String
   */
  private[registry] def payloadBodyToEvents(body: String): Either[FailureDetails.AdapterFailure, List[Json]] =
    for {
      b <- JsonUtils
        .extractJson(body)
        .leftMap(e => FailureDetails.AdapterFailure.NotJson("body", body.some, e))
      a <- b.asArray.toRight(
        FailureDetails.AdapterFailure.InputData("body", body.some, "not a json array")
      )
    } yield a.toList

  /**
   * Returns an updated HubSpot event JSON where the "subscriptionType" field is removed and
   * "occurredAt" fields' values have been converted
   * @param json The event JSON which we need to update values for
   * @return the updated JSON with updated fields and values
   */
  def reformatParameters(json: Json): Json = {
    def toStringField(value: Long): String = {
      val dt: DateTime = new DateTime(value)
      JsonSchemaDateTimeFormat.print(dt)
    }

    val longToDateString: Kleisli[Option, Json, Json] = Kleisli(
      (json: Json) =>
        json
          .as[Long]
          .toOption
          .map(v => Json.fromString(toStringField(v)))
    )

    val occurredAtKey = "occurredAt"
    (for {
      jObj <- json.asObject
      newValue = jObj.kleisli
        .andThen(longToDateString)
        .run(occurredAtKey)
      res = newValue
        .map(v => jObj.add(occurredAtKey, v))
        .getOrElse(jObj)
        .remove("subscriptionType")
    } yield Json.fromJsonObject(res)).getOrElse(json)
  }
}
