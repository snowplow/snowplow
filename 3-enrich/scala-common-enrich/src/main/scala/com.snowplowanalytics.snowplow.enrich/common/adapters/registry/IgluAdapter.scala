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

import java.net.URI
import java.nio.charset.StandardCharsets.UTF_8

import scala.collection.JavaConverters._

import cats.Monad
import cats.data.{NonEmptyList, ValidatedNel}
import cats.effect.Clock
import cats.syntax.validated._
import com.snowplowanalytics.iglu.client.Client
import com.snowplowanalytics.iglu.client.resolver.registries.RegistryLookup
import com.snowplowanalytics.iglu.core.SchemaKey
import io.circe._
import io.circe.parser._
import io.circe.syntax._
import org.apache.http.client.utils.URLEncodedUtils

import loaders.CollectorPayload

/**
 * Transforms a collector payload which either:
 * 1. Provides a set of kv pairs on a GET querystring with a &schema={iglu schema uri} parameter.
 * 2. Provides a &schema={iglu schema uri} parameter on a POST querystring and a set of kv pairs in
 * the body.
 *    - Formatted as JSON
 *    - Formatted as a Form Body
 */
object IgluAdapter extends Adapter {
  // Vendor name for Failure Message
  private val VendorName = "Iglu"

  // Tracker version for an Iglu-compatible webhook
  private val TrackerVersion = "com.snowplowanalytics.iglu-v1"

  // Create a simple formatter function
  private val IgluFormatter: FormatterFunc = buildFormatter() // For defaults

  /**
   * Converts a CollectorPayload instance into raw events. Currently we only support a single event
   * Iglu-compatible self-describing event passed in on the querystring.
   * @param payload The CollectorPaylod containing one or more raw events as collected by a Snowplow
   * collector
   * @param client The Iglu client used for schema lookup and validation
   * @return a Validation boxing either a NEL of RawEvents on Success, or a NEL of Failure Strings
   */
  override def toRawEvents[F[_]: Monad: RegistryLookup: Clock](
    payload: CollectorPayload,
    client: Client[F, Json]
  ): F[ValidatedNel[String, NonEmptyList[RawEvent]]] = {
    val _ = client
    val params = toMap(payload.querystring)
    (params.get("schema"), payload.body, payload.contentType) match {
      case (_, Some(_), None) =>
        Monad[F].pure(
          s"$VendorName event failed: ContentType must be set for a POST payload".invalidNel
        )
      case (None, Some(body), Some(contentType)) =>
        Monad[F].pure(payloadSdJsonToEvent(payload, body, contentType, params))
      case (Some(schemaUri), Some(_), Some(_)) =>
        Monad[F].pure(payloadToEventWithSchema(payload, schemaUri, params))
      case (Some(schemaUri), None, _) =>
        Monad[F].pure(payloadToEventWithSchema(payload, schemaUri, params))
      case (_, _, _) =>
        Monad[F].pure(
          s"$VendorName event failed: is not a sd-json or a valid GET or POST request".invalidNel
        )
    }
  }

  // --- SelfDescribingJson Payloads

  /**
   * Processes a potential SelfDescribingJson into a validated raw-event.
   * @param payload The CollectorPaylod containing one or more raw events
   * @param body The extracted body string
   * @param contentType The extracted contentType string
   * @param params The raw map of params from the querystring.
   */
  private[registry] def payloadSdJsonToEvent(
    payload: CollectorPayload,
    body: String,
    contentType: String,
    params: Map[String, String]
  ): ValidatedNel[String, NonEmptyList[RawEvent]] =
    contentType match {
      case "application/json" => sdJsonBodyToEvent(payload, body, params)
      case "application/json; charset=utf-8" => sdJsonBodyToEvent(payload, body, params)
      case _ => "Content type not supported".invalidNel
    }

  /**
   * Processes a potential SelfDescribingJson into a validated raw-event.
   * @param payload The CollectorPaylod containing one or more raw events
   * @param body The extracted body string
   * @param params The raw map of params from the querystring.
   */
  private[registry] def sdJsonBodyToEvent(
    payload: CollectorPayload,
    body: String,
    params: Map[String, String]
  ): ValidatedNel[String, NonEmptyList[RawEvent]] =
    parseJsonSafe(body) match {
      case Right(parsed) =>
        val cursor = parsed.hcursor
        (cursor.get[String]("schema").toOption, cursor.downField("data").focus) match {
          case (Some(schemaUri), Some(data)) =>
            SchemaKey.fromUri(schemaUri) match {
              case Left(parseError) => parseError.code.invalidNel
              case _ =>
                NonEmptyList
                  .one(
                    RawEvent(
                      api = payload.api,
                      parameters =
                        toUnstructEventParams(TrackerVersion, params, schemaUri, data, "app"),
                      contentType = payload.contentType,
                      source = payload.source,
                      context = payload.context
                    )
                  )
                  .valid
            }
          case (None, _) =>
            s"$VendorName event failed: detected SelfDescribingJson but schema key is missing".invalidNel
          case (_, None) =>
            s"$VendorName event failed: detected SelfDescribingJson but data key is missing".invalidNel
        }
      case Left(err) => err.invalidNel
    }

  // --- Payloads with the Schema in the Query-String

  /**
   * Processes a payload that has the schema field in the query-string.
   * @param payload The CollectorPaylod containing one or more raw events
   * @param schemaUri The schema-uri found
   * @param params The raw map of params from the querystring.
   */
  private[registry] def payloadToEventWithSchema(
    payload: CollectorPayload,
    schemaUri: String,
    params: Map[String, String]
  ): ValidatedNel[String, NonEmptyList[RawEvent]] =
    SchemaKey.fromUri(schemaUri) match {
      case Left(parseError) => parseError.code.invalidNel
      case Right(_) =>
        (payload.body, payload.contentType) match {
          case (None, _) =>
            NonEmptyList
              .one(
                RawEvent(
                  api = payload.api,
                  parameters = toUnstructEventParams(
                    TrackerVersion,
                    (params - "schema"),
                    schemaUri,
                    IgluFormatter,
                    "app"
                  ),
                  contentType = payload.contentType,
                  source = payload.source,
                  context = payload.context
                )
              )
              .valid
          case (Some(body), Some(contentType)) =>
            contentType match {
              case "application/json" => jsonBodyToEvent(payload, body, schemaUri, params)
              case "application/json; charset=utf-8" =>
                jsonBodyToEvent(payload, body, schemaUri, params)
              case "application/x-www-form-urlencoded" =>
                formBodyToEvent(payload, body, schemaUri, params)
              case _ => "Content type not supported".invalidNel
            }
          case (_, None) => "Content type has not been specified".invalidNel
        }
    }

  /**
   * Converts a json payload into a single validated event
   * @param body json payload as POST'd by a webhook
   * @param payload the rest of the payload details
   * @param schemaUri the schemaUri for the event
   * @param params The query string parameters
   * @return a single validated event
   */
  private[registry] def jsonBodyToEvent(
    payload: CollectorPayload,
    body: String,
    schemaUri: String,
    params: Map[String, String]
  ): ValidatedNel[String, NonEmptyList[RawEvent]] = {
    def buildRawEvent(e: Json): RawEvent =
      RawEvent(
        api = payload.api,
        parameters = toUnstructEventParams(TrackerVersion, (params - "schema"), schemaUri, e, "app"),
        contentType = payload.contentType,
        source = payload.source,
        context = payload.context
      )

    parse(body) match {
      case Right(parsed) =>
        parsed.asArray match {
          case Some(array) =>
            array.toList match {
              case h :: t => NonEmptyList.of(buildRawEvent(h), t.map(buildRawEvent): _*).valid
              case _ =>
                s"$VendorName event failed json sanity check: array of events cannot be empty".invalidNel
            }
          case _ =>
            if (parsed.asObject.fold(true)(_.isEmpty)) {
              s"$VendorName event failed json sanity check: has no key-value pairs".invalidNel
            } else {
              NonEmptyList.one(buildRawEvent(parsed)).valid
            }
        }
      case Left(err) => err.getMessage.invalidNel
    }
  }

  /**
   * Converts a form body payload into a single validated event
   * @param body the form body from the payload as POST'd by a webhook
   * @param payload the rest of the payload details
   * @param schemaUri the schemaUri for the event
   * @param params The query string parameters
   * @return a single validated event
   */
  private[registry] def formBodyToEvent(
    payload: CollectorPayload,
    body: String,
    schemaUri: String,
    params: Map[String, String]
  ): ValidatedNel[String, NonEmptyList[RawEvent]] = {
    val bodyMap =
      toMap(URLEncodedUtils.parse(URI.create("http://localhost/?" + body), UTF_8).asScala.toList)
    val event = bodyMap.asJson

    NonEmptyList
      .one(
        RawEvent(
          api = payload.api,
          parameters =
            toUnstructEventParams(TrackerVersion, (params - "schema"), schemaUri, event, "srv"),
          contentType = payload.contentType,
          source = payload.source,
          context = payload.context
        )
      )
      .valid
  }
}
