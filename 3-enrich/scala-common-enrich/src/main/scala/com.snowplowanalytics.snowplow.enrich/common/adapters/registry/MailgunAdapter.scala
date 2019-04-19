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
import io.circe._
import io.circe.syntax._
import org.apache.http.client.utils.URLEncodedUtils

import loaders.CollectorPayload
import utils.{JsonUtils => JU}

/**
 * Transforms a collector payload which conforms to a known version of the StatusGator Tracking
 * webhook into raw events.
 */
object MailgunAdapter extends Adapter {
  // Vendor name for Failure Message
  private val VendorName = "Mailgun"

  // Tracker version for an Mailgun Tracking webhook
  private val TrackerVersion = "com.mailgun-v1"

  // Expected content type for a request body
  private val ContentTypes = List("application/x-www-form-urlencoded", "multipart/form-data")
  private val ContentTypesStr = ContentTypes.mkString(" or ")

  private val Vendor = "com.mailgun"
  private val Format = "jsonschema"
  private val SchemaVersion = SchemaVer.Full(1, 0, 0)

  // Schemas for reverse-engineering a Snowplow unstructured event
  private val EventSchemaMap = Map(
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
  override def toRawEvents[F[_]: Monad: RegistryLookup: Clock](
    payload: CollectorPayload,
    client: Client[F, Json]
  ): F[ValidatedNel[String, NonEmptyList[RawEvent]]] =
    (payload.body, payload.contentType) match {
      case (None, _) =>
        Monad[F].pure(s"Request body is empty: no $VendorName events to process".invalidNel)
      case (_, None) =>
        Monad[F].pure(
          s"Request body provided but content type empty, expected $ContentTypesStr for $VendorName".invalidNel
        )
      case (_, Some(ct)) if !ContentTypes.exists(ct.startsWith(_)) =>
        Monad[F].pure(
          s"Content type of $ct provided, expected $ContentTypesStr for $VendorName".invalidNel
        )
      case (Some(body), _) if (body.isEmpty) =>
        Monad[F].pure(s"$VendorName event body is empty: nothing to process".invalidNel)
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
            val message = JU.stripInstanceEtc(e.getMessage).orNull
            Monad[F].pure(s"$VendorName adapter could not parse body: [$message]".invalidNel)
          case TS(bodyMap) =>
            Monad[F].pure(
              bodyMap
                .get("event")
                .map { eventType =>
                  (for {
                    schemaUri <- lookupSchema(eventType.some, VendorName, EventSchemaMap)
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
                .getOrElse(
                  s"No $VendorName event parameter provided: cannot determine event type".invalidNel
                )
            )
        }
    }

  /**
   * Adds, removes and converts input fields to output fields
   * @param json parsed event fields as a JValue
   * @return The mutated event.
   */
  private def mutateMailgunEvent(json: Json): Either[String, Json] = {
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
      case _ => s"$VendorName event string is not a json object".asLeft
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
  private def payloadBodyToEvent(bodyMap: Map[String, String]): Either[String, Json] =
    (bodyMap.get("timestamp"), bodyMap.get("token"), bodyMap.get("signature")) match {
      case (None, _, _) => s"$VendorName event data missing 'timestamp'".asLeft
      case (_, None, _) => s"$VendorName event data missing 'token'".asLeft
      case (_, _, None) => s"$VendorName event data missing 'signature'".asLeft
      case (Some(_), Some(_), Some(_)) => bodyMap.asJson.asRight
    }
}
