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

import scala.collection.JavaConverters._
import scala.util.{Try, Success => TS, Failure => TF}

import cats.Monad
import cats.data.{NonEmptyList, ValidatedNel}
import cats.instances.either._
import cats.syntax.either._
import cats.syntax.option._
import cats.syntax.validated._

import cats.effect.Clock

import com.snowplowanalytics.iglu.client.Client
import com.snowplowanalytics.iglu.client.resolver.registries.RegistryLookup
import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer}
import com.snowplowanalytics.snowplow.badrows.FailureDetails

import io.circe._
import io.circe.optics.JsonPath._

import org.apache.http.client.utils.URLEncodedUtils
import org.joda.time.DateTime

import loaders.CollectorPayload
import utils.{HttpClient, JsonUtils => JU}

/**
 * Transforms a collector payload which conforms to a known version of the Olark Tracking webhook
 * into raw events.
 */
object OlarkAdapter extends Adapter {
  // Tracker version for an Olark Tracking webhook
  private val TrackerVersion = "com.olark-v1"

  // Expected content type for a request body
  private val ContentType = "application/x-www-form-urlencoded"

  private val Vendor = "com.olark"
  private val Format = "jsonschema"
  private val SchemaVersion = SchemaVer.Full(1, 0, 0)

  // Schemas for reverse-engineering a Snowplow unstructured event
  private val EventSchemaMap = Map(
    "transcript" -> SchemaKey(Vendor, "transcript", Format, SchemaVersion),
    "offline_message" -> SchemaKey(Vendor, "offline_message", Format, SchemaVersion)
  )

  /**
   * Converts a CollectorPayload instance into raw events. An Olark Tracking payload contains one
   * single event in the body of the payload, stored within a HTTP encoded string.
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
      case (Some(body), _) if (body.isEmpty) =>
        val failure = FailureDetails.AdapterFailure.InputData(
          "body",
          none,
          "empty body: no events to process"
        )
        Monad[F].pure(failure.invalidNel)
      case (_, None) =>
        val msg = s"no content type: expected $ContentType"
        Monad[F].pure(
          FailureDetails.AdapterFailure.InputData("contentType", none, msg).invalidNel
        )
      case (_, Some(ct)) if ct != ContentType =>
        val msg = s"expected $ContentType"
        Monad[F].pure(
          FailureDetails.AdapterFailure.InputData("contentType", none, msg).invalidNel
        )
      case (Some(body), _) =>
        val _ = client
        val qsParams = toMap(payload.querystring)
        Try {
          toMap(
            URLEncodedUtils.parse(URI.create("http://localhost/?" + body), UTF_8).asScala.toList
          )
        } match {
          case TF(e) =>
            val msg = s"could not parse body: ${JU.stripInstanceEtc(e.getMessage).orNull}"
            Monad[F].pure(
              FailureDetails.AdapterFailure.InputData(body, body.some, msg).invalidNel
            )
          case TS(bodyMap) =>
            Monad[F].pure(
              (for {
                event <- payloadBodyToEvent(bodyMap)
                eventType = event.hcursor.get[Json]("operators").toOption match {
                  case Some(_) => "transcript"
                  case _ => "offline_message"
                }
                schema <- lookupSchema(eventType.some, EventSchemaMap)
                transformedEvent <- transformTimestamps(event)
              } yield NonEmptyList.one(
                RawEvent(
                  api = payload.api,
                  parameters = toUnstructEventParams(
                    TrackerVersion,
                    qsParams,
                    schema,
                    camelize(transformedEvent),
                    "srv"
                  ),
                  contentType = payload.contentType,
                  source = payload.source,
                  context = payload.context
                )
              )).toValidatedNel
            )
        }
    }

  /**
   * Converts all olark timestamps in a parsed transcript or offline_message json object to iso8601
   * strings
   * @param json a parsed event
   * @return JObject the event with timstamps replaced
   */
  private def transformTimestamps(json: Json): Either[FailureDetails.AdapterFailure, Json] = {
    def toMsec(oTs: String): Either[FailureDetails.AdapterFailure, Long] =
      for {
        formatted <- oTs.split('.') match {
          case Array(sec) => s"${sec}000".asRight
          case Array(sec, msec) => s"${sec}${msec.take(3).padTo(3, '0')}".asRight
          case _ =>
            FailureDetails.AdapterFailure
              .InputData("timestamp", oTs.some, "unexpected timestamp format")
              .asLeft
        }
        long <- Either
          .catchNonFatal(formatted.toLong)
          .leftMap { _ =>
            FailureDetails.AdapterFailure.InputData(
              "timestamp",
              formatted.some,
              "cannot be converted to Double"
            )
          }
      } yield long

    type EitherAF[A] = Either[FailureDetails.AdapterFailure, A]
    root.items.each.timestamp.string.modifyF[EitherAF] { v =>
      toMsec(v).map(long => JsonSchemaDateTimeFormat.print(new DateTime(long)))
    }(json)
  }

  /**
   * Converts a querystring payload into an event
   * @param bodyMap The converted map from the querystring
   */
  private def payloadBodyToEvent(
    bodyMap: Map[String, String]
  ): Either[FailureDetails.AdapterFailure, Json] =
    bodyMap.get("data") match {
      case None | Some("") =>
        FailureDetails.AdapterFailure
          .InputData("data", none, "missing 'data' field")
          .asLeft
      case Some(json) =>
        JU.extractJson(json)
          .leftMap(e => FailureDetails.AdapterFailure.NotJson("data", json.some, e))
    }
}
