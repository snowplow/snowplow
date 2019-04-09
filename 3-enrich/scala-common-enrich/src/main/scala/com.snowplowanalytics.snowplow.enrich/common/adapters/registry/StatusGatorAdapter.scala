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
import cats.effect.Clock
import cats.syntax.validated._
import com.snowplowanalytics.iglu.client.Client
import com.snowplowanalytics.iglu.client.resolver.registries.RegistryLookup
import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer}
import io.circe.Json
import io.circe.syntax._
import org.apache.http.client.utils.URLEncodedUtils

import loaders.CollectorPayload
import utils.{JsonUtils => JU}

/**
 * Transforms a collector payload which conforms to a known version of the StatusGator Tracking
 * webhook into raw events.
 */
object StatusGatorAdapter extends Adapter {
  // Vendor name for Failure Message
  private val VendorName = "StatusGator"

  // Tracker version for an StatusGator Tracking webhook
  private val TrackerVersion = "com.statusgator-v1"

  // Expected content type for a request body
  private val ContentType = "application/x-www-form-urlencoded"

  // Schemas for reverse-engineering a Snowplow unstructured event
  private val EventSchema =
    SchemaKey("com.statusgator", "status_change", "jsonschema", SchemaVer.Full(1, 0, 0)).toSchemaUri

  /**
   * Converts a CollectorPayload instance into raw events. A StatusGator Tracking payload contains
   * one single event in the body of the payload, stored within a HTTP encoded string.
   * @param payload The CollectorPayload containing one or more raw events
   * @param client The Iglu client used for schema lookup and validation
   * @return a Validation boxing either a NEL of RawEvents on Success, or a NEL of Failure Strings
   */
  override def toRawEvents[F[_]: Monad: RegistryLookup: Clock](
    payload: CollectorPayload,
    client: Client[F, Json]
  ): F[ValidatedNel[String, NonEmptyList[RawEvent]]] =
    (payload.body, payload.contentType) match {
      case (None, _) => Monad[F].pure(
        s"Request body is empty: no $VendorName events to process".invalidNel)
      case (_, None) => Monad[F].pure(
        s"Request body provided but content type empty, expected $ContentType for $VendorName"
          .invalidNel)
      case (_, Some(ct)) if ct != ContentType => Monad[F].pure(
        s"Content type of $ct provided, expected $ContentType for $VendorName".invalidNel)
      case (Some(body), _) if (body.isEmpty) => Monad[F].pure(
        s"$VendorName event body is empty: nothing to process".invalidNel)
      case (Some(body), _) =>
        val _ = client
        val qsParams = toMap(payload.querystring)
        Try {
          toMap(URLEncodedUtils.parse(URI.create("http://localhost/?" + body), UTF_8).asScala.toList)
        } match {
          case TF(e) =>
            val msg = JU.stripInstanceEtc(e.getMessage).orNull
            Monad[F].pure(s"$VendorName incorrect event string : [$msg]".invalidNel)
          case TS(bodyMap) =>
            Monad[F].pure(NonEmptyList
              .one(
                RawEvent(
                  api = payload.api,
                  parameters = toUnstructEventParams(
                    TrackerVersion,
                    qsParams,
                    EventSchema,
                    camelize(bodyMap.asJson),
                    "srv"),
                  contentType = payload.contentType,
                  source = payload.source,
                  context = payload.context
                )
              )
              .valid)
        }
    }
}
