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
import cats.syntax.either._
import cats.syntax.option._
import cats.syntax.validated._
import com.snowplowanalytics.iglu.client.Client
import com.snowplowanalytics.iglu.client.resolver.registries.RegistryLookup
import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer}
import com.snowplowanalytics.snowplow.badrows._
import io.circe._
import io.circe.syntax._
import org.apache.http.client.utils.URLEncodedUtils

import loaders.CollectorPayload
import utils.{HttpClient, JsonUtils => JU}

/**
 * Transforms a collector payload which conforms to a known version of the StatusGator Tracking
 * webhook into raw events.
 */
object MailgunAdapter extends Adapter {
  // Tracker version for an Mailgun Tracking webhook
  private val TrackerVersion = "com.mailgun-v1"

  // Expected content type for a request body
  private val ContentTypes = List("application/x-www-form-urlencoded", "multipart/form-data")
  private val ContentTypesStr = ContentTypes.mkString(", ")

  private val Vendor = "com.mailgun"
  private val Format = "jsonschema"
  private val SchemaVersion = SchemaVer.Full(1, 0, 0)

  // Schemas for reverse-engineering a Snowplow unstructured event
  private[registry] val EventSchemaMap = Map(
    "bounced" -> SchemaKey(Vendor, "message_bounced", Format, SchemaVersion).toSchemaUri,
    "clicked" -> SchemaKey(Vendor, "message_clicked", Format, SchemaVersion).toSchemaUri,
    "complained" -> SchemaKey(Vendor, "message_complained", Format, SchemaVersion).toSchemaUri,
    "delivered" -> SchemaKey(Vendor, "message_delivered", Format, SchemaVersion).toSchemaUri,
    "dropped" -> SchemaKey(Vendor, "message_dropped", Format, SchemaVersion).toSchemaUri,
    "opened" -> SchemaKey(Vendor, "message_opened", Format, SchemaVersion).toSchemaUri,
    "unsubscribed" -> SchemaKey(Vendor, "recipient_unsubscribed", Format, SchemaVersion).toSchemaUri
  )

  /**
   * A Mailgun Tracking payload contains one single event in the body of the payload, stored within
   * a HTTP encoded string.
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
        val msg = s"no content type: expected one of $ContentTypesStr"
        Monad[F].pure(
          FailureDetails.AdapterFailure.InputData("contentType", none, msg).invalidNel
        )
      case (_, Some(ct)) if !ContentTypes.exists(ct.startsWith(_)) =>
        val msg = s"expected one of $ContentTypesStr"
        Monad[F].pure(
          FailureDetails.AdapterFailure
            .InputData("contentType", ct.some, msg)
            .invalidNel
        )
      case (Some(body), Some(ct)) =>
        val _ = client
        val params = toMap(payload.querystring)
        Try {
          getBoundary(ct)
            .map(parseMultipartForm(body, _))
            .getOrElse(
              toMap(
                URLEncodedUtils.parse(URI.create("http://localhost/?" + body), UTF_8).asScala.toList
              )
            )
        } match {
          case TF(e) =>
            val msg = s"could not parse body: ${JU.stripInstanceEtc(e.getMessage).orNull}"
            Monad[F].pure(
              FailureDetails.AdapterFailure
                .InputData("body", body.some, msg)
                .invalidNel
            )
          case TS(bodyMap) =>
            Monad[F].pure(
              bodyMap
                .get("event")
                .map { eventType =>
                  (for {
                    schemaUri <- lookupSchema(eventType.some, EventSchemaMap)
                    event <- payloadBodyToEvent(bodyMap)
                    mEvent <- mutateMailgunEvent(event)
                  } yield NonEmptyList.one(
                    RawEvent(
                      api = payload.api,
                      parameters = toUnstructEventParams(
                        TrackerVersion,
                        params,
                        schemaUri,
                        cleanupJsonEventValues(
                          mEvent,
                          ("event", eventType).some,
                          List("timestamp")
                        ),
                        "srv"
                      ),
                      contentType = payload.contentType,
                      source = payload.source,
                      context = payload.context
                    )
                  )).toValidatedNel
                }
                .getOrElse {
                  val msg = "no `event` parameter provided: cannot determine event type"
                  FailureDetails.AdapterFailure
                    .InputData("body", body.some, msg)
                    .invalidNel
                }
            )
        }
    }

  /**
   * Adds, removes and converts input fields to output fields
   * @param json parsed event fields as a JValue
   * @return The mutated event.
   */
  private def mutateMailgunEvent(json: Json): Either[FailureDetails.AdapterFailure, Json] = {
    val attachmentCountKey = "attachmentCount"
    val camelCase = camelize(json)
    camelCase.asObject match {
      case Some(obj) =>
        val withFilteredFields = obj
          .filterKeys(name => !(name == "bodyPlain" || name == attachmentCountKey))
        val attachmentCount = for {
          acJson <- obj(attachmentCountKey)
          acInt <- acJson.as[Int].toOption
        } yield acInt
        val finalJsonObject = attachmentCount match {
          case Some(ac) => withFilteredFields.add(attachmentCountKey, Json.fromInt(ac))
          case _ => withFilteredFields
        }
        Json.fromJsonObject(finalJsonObject).asRight
      case _ =>
        FailureDetails.AdapterFailure
          .InputData("body", json.noSpaces.some, "body is not a json object")
          .asLeft
    }
  }

  private val boundaryRegex =
    """multipart/form-data.*?boundary=(?:")?([\S ]{0,69})(?: )*(?:")?$""".r

  /**
   * Returns the boundary parameter for a message of media type multipart/form-data
   * (https://www.ietf.org/rfc/rfc2616.txt and https://www.ietf.org/rfc/rfc2046.txt)
   * @param contentType Header field of the form
   * "multipart/form-data; boundary=353d603f-eede-4b49-97ac-724fbc54ea3c"
   * @return boundary Option[String]
   */
  private def getBoundary(contentType: String): Option[String] = contentType match {
    case boundaryRegex(boundaryString) => Some(boundaryString)
    case _ => None
  }

  /**
   * Rudimentary parsing the form fields of a multipart/form-data into a Map[String, String]
   * other fields will be discarded
   * (see https://www.ietf.org/rfc/rfc1867.txt and https://www.ietf.org/rfc/rfc2046.txt).
   * This parser will only take into account part headers of content-disposition type form-data
   * and only the parameter name e.g.
   * Content-Disposition: form-data; anything="notllokingintothis"; name="key"
   *
   * value
   * @param body The body of the message
   * @param boundary String that separates the body parts
   * @return a map of the form fields and their values (other fields are dropped)
   */
  private def parseMultipartForm(body: String, boundary: String): Map[String, String] =
    body
      .split(s"--$boundary")
      .flatMap({
        case formDataRegex(k, v) => Some((k, v))
        case _ => None
      })
      .toMap

  private val formDataRegex =
    """(?sm).*Content-Disposition:\s*form-data\s*;[ \S\t]*?name="([^"]+)"[ \S\t]*$.*?(?<=^[ \t\S]*$)^\s*(.*?)(?:\s*)\z""".r

  /**
   * Converts a querystring payload into an event
   * @param bodyMap The converted map from the querystring
   */
  private def payloadBodyToEvent(
    bodyMap: Map[String, String]
  ): Either[FailureDetails.AdapterFailure, Json] =
    (bodyMap.get("timestamp"), bodyMap.get("token"), bodyMap.get("signature")) match {
      case (None, _, _) =>
        FailureDetails.AdapterFailure
          .InputData("timestamp", none, "missing 'timestamp'")
          .asLeft
      case (_, None, _) =>
        FailureDetails.AdapterFailure
          .InputData("token", none, "missing 'token'")
          .asLeft
      case (_, _, None) =>
        FailureDetails.AdapterFailure
          .InputData("signature", none, "missing 'signature'")
          .asLeft
      case (Some(_), Some(_), Some(_)) => bodyMap.asJson.asRight
    }
}
