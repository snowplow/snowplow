/*
 * Copyright (c) 2016-2020 Snowplow Analytics Ltd. All rights reserved.
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
import cats.effect.Clock
import cats.syntax.apply._
import cats.syntax.either._
import cats.syntax.option._
import cats.syntax.validated._
import com.snowplowanalytics.iglu.client.Client
import com.snowplowanalytics.iglu.client.resolver.registries.RegistryLookup
import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer}
import com.snowplowanalytics.snowplow.badrows._
import io.circe._
import org.apache.http.client.utils.URLEncodedUtils

import loaders.CollectorPayload
import utils.{HttpClient, JsonUtils => JU}

/**
 * Transforms a collector payload which conforms to a known version of the Unbounce Tracking webhook
 * into raw events.
 */
object UnbounceAdapter extends Adapter {
  // Tracker version for an Unbounce Tracking webhook
  private val TrackerVersion = "com.unbounce-v1"

  // Expected content type for a request body
  private val ContentType = "application/x-www-form-urlencoded"

  // Schema for Unbounce event context
  private val ContextSchema = Map(
    "form_post" -> SchemaKey("com.unbounce", "form_post", "jsonschema", SchemaVer.Full(1, 0, 0))
  )

  /**
   * Converts a CollectorPayload instance into raw events. An Unbounce Tracking payload contains one
   * single event in the body of the payload, stored within a HTTP encoded string.
   * @param payload The CollectorPayload containing one or more raw events
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
          FailureDetails.AdapterFailure
            .InputData("contentType", ct.some, msg)
            .invalidNel
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
              (
                payloadBodyToEvent(bodyMap).toValidatedNel,
                lookupSchema(Some("form_post"), ContextSchema).toValidatedNel
              ).mapN { (event, schema) =>
                NonEmptyList.one(
                  RawEvent(
                    api = payload.api,
                    parameters = toUnstructEventParams(TrackerVersion, qsParams, schema, event, "srv"),
                    contentType = payload.contentType,
                    source = payload.source,
                    context = payload.context
                  )
                )
              }
            )
        }
    }

  private def payloadBodyToEvent(bodyMap: Map[String, String]): Either[FailureDetails.AdapterFailure, Json] =
    (
      bodyMap.get("page_id"),
      bodyMap.get("page_name"),
      bodyMap.get("variant"),
      bodyMap.get("page_url"),
      bodyMap.get("data.json")
    ) match {
      case (None, _, _, _, _) =>
        FailureDetails.AdapterFailure
          .InputData("page_id", none, "missing 'page_id' field in body")
          .asLeft
      case (_, None, _, _, _) =>
        FailureDetails.AdapterFailure
          .InputData("page_name", none, "missing 'page_name' field in body")
          .asLeft
      case (_, _, None, _, _) =>
        FailureDetails.AdapterFailure
          .InputData("variant", none, "missing 'variant' field in body")
          .asLeft
      case (_, _, _, None, _) =>
        FailureDetails.AdapterFailure
          .InputData("page_url", none, "missing 'page_url' field in body")
          .asLeft
      case (_, _, _, _, None) =>
        FailureDetails.AdapterFailure
          .InputData("data.json", none, "missing 'data.json' field in body")
          .asLeft
      case (_, _, _, _, Some(dataJson)) if dataJson.isEmpty =>
        FailureDetails.AdapterFailure
          .InputData("data.json", none, "empty 'data.json' field in body")
          .asLeft
      case (Some(_), Some(_), Some(_), Some(_), Some(dataJson)) =>
        val event = (bodyMap - "data.json" - "data.xml").toList
        JU.extractJson(dataJson)
          .map { dJs =>
            val js = Json
              .obj(
                ("data.json", dJs) :: event.map { case (k, v) => (k, Json.fromString(v)) }: _*
              )
            camelize(js)
          }
          .leftMap(
            e => FailureDetails.AdapterFailure.NotJson("data.json", dataJson.some, e)
          )
    }
}
