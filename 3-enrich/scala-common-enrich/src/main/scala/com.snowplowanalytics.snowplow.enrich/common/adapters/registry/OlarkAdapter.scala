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

import scala.collection.JavaConversions._
import scala.util.{Try, Success => TS, Failure => TF}

import cats.Monad
import cats.data.{NonEmptyList, ValidatedNel}
import cats.instances.either._
import cats.syntax.either._
import cats.syntax.option._
import cats.syntax.validated._
import com.snowplowanalytics.iglu.client.Client
import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer}
import io.circe._
import io.circe.parser._
import io.circe.optics.JsonPath._
import org.apache.http.client.utils.URLEncodedUtils
import org.joda.time.DateTime

import loaders.CollectorPayload
import utils.{JsonUtils => JU}

/**
 * Transforms a collector payload which conforms to a known version of the Olark Tracking webhook
 * into raw events.
 */
object OlarkAdapter extends Adapter {
  // Vendor name for Failure Message
  private val VendorName = "Olark"

  // Tracker version for an Olark Tracking webhook
  private val TrackerVersion = "com.olark-v1"

  // Expected content type for a request body
  private val ContentType = "application/x-www-form-urlencoded"

  private val Vendor = "com.olark"
  private val Format = "jsonschema"
  private val SchemaVersion = SchemaVer.Full(1, 0, 0)

  // Schemas for reverse-engineering a Snowplow unstructured event
  private val EventSchemaMap = Map(
    "transcript" -> SchemaKey(Vendor, "transcript", Format, SchemaVersion).toSchemaUri,
    "offline_message" -> SchemaKey(Vendor, "offline_message", Format, SchemaVersion).toSchemaUri
  )

  /**
   * Converts a CollectorPayload instance into raw events. An Olark Tracking payload contains one
   * single event in the body of the payload, stored within a HTTP encoded string.
   * @param payload The CollectorPayload containing one or more raw events
   * @param client The Iglu client used for schema lookup and validation
   * @return a Validation boxing either a NEL of RawEvents on Success, or a NEL of Failure Strings
   */
  override def toRawEvents[F[_]: Monad](
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
      case (Some(body), _) if (body.isEmpty) =>
        Monad[F].pure(s"$VendorName event body is empty: nothing to process".invalidNel)
      case (Some(body), _) =>
        val _ = client
        val qsParams = toMap(payload.querystring)
        Try {
          toMap(URLEncodedUtils.parse(URI.create("http://localhost/?" + body), UTF_8).toList)
        } match {
          case TF(e) =>
            val message = JU.stripInstanceEtc(e.getMessage).orNull
            Monad[F].pure(s"$VendorName could not parse body: [$message]".invalidNel)
          case TS(bodyMap) =>
            Monad[F].pure((for {
              event <- payloadBodyToEvent(bodyMap)
              eventType = event.hcursor.get[Json]("operators").toOption match {
                case Some(_) => "transcript"
                case _ => "offline_message"
              }
              schema <- lookupSchema(eventType.some, VendorName, EventSchemaMap)
              transformedEvent <- transformTimestamps(event)
            } yield
              NonEmptyList.one(
                RawEvent(
                  api = payload.api,
                  parameters = toUnstructEventParams(
                    TrackerVersion,
                    qsParams,
                    schema,
                    camelize(transformedEvent),
                    "srv"),
                  contentType = payload.contentType,
                  source = payload.source,
                  context = payload.context
                )
              )).toValidatedNel)
        }
    }

  /**
   * Converts all olark timestamps in a parsed transcript or offline_message json object to iso8601
   * strings
   * @param json a parsed event
   * @return JObject the event with timstamps replaced
   */
  private def transformTimestamps(json: Json): Either[String, Json] = {
    def toMsec(oTs: String): Either[String, Long] =
      for {
        formatted <- oTs.split('.') match {
          case Array(sec) => Right(s"${sec}000")
          case Array(sec, msec) => Right(s"${sec}${msec.take(3).padTo(3, '0')}")
          case _ => Left(s"$VendorName unexpected timestamp format: $oTs")
        }
        long <- Either.catchNonFatal(formatted.toLong).leftMap(_.getMessage)
      } yield long

    type EitherString[A] = Either[String, A]

    val modifiedTimestamps: Either[String, Json] =
      root.items.each.timestamp.string.modifyF[EitherString] { v =>
        toMsec(v).map(long => JsonSchemaDateTimeFormat.print(new DateTime(long)))
      }(json)

    modifiedTimestamps match {
      case Right(json) => json.asRight
      case Left(e) => s"$VendorName could not convert timestamps: [$e]".asLeft
    }
  }

  /**
   * Converts a querystring payload into an event
   * @param bodyMap The converted map from the querystring
   */
  private def payloadBodyToEvent(bodyMap: Map[String, String]): Either[String, Json] =
    bodyMap.get("data") match {
      case None => s"$VendorName event data does not have 'data' as a key".asLeft
      case Some("") => s"$VendorName event data is empty: nothing to process".asLeft
      case Some(json) =>
        parse(json) match {
          case Right(event) => event.asRight
          case Left(e) =>
            s"$VendorName event string failed to parse into JSON: [${e.getMessage}]".asLeft
        }
    }
}
