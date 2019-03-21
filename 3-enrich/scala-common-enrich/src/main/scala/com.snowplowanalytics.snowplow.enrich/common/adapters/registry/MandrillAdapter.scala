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

import scala.collection.JavaConversions._

import cats.data.{NonEmptyList, ValidatedNel}
import cats.syntax.either._
import cats.syntax.validated._
import com.snowplowanalytics.iglu.client.{Resolver, SchemaKey}
import io.circe._
import io.circe.parser._
import org.apache.http.client.utils.URLEncodedUtils

import loaders.CollectorPayload

/**
 * Transforms a collector payload which conforms to a known version of the Mandrill Tracking webhook
 * into raw events.
 */
object MandrillAdapter extends Adapter {
  // Vendor name for Failure Message
  private val VendorName = "Mandrill"

  // Tracker version for an Mandrill Tracking webhook
  private val TrackerVersion = "com.mandrill-v1"

  // Expected content type for a request body
  private val ContentType = "application/x-www-form-urlencoded"

  // Schemas for reverse-engineering a Snowplow unstructured event
  private val EventSchemaMap = Map(
    "hard_bounce" -> SchemaKey("com.mandrill", "message_bounced", "jsonschema", "1-0-1").toSchemaUri,
    "click" -> SchemaKey("com.mandrill", "message_clicked", "jsonschema", "1-0-1").toSchemaUri,
    "deferral" -> SchemaKey("com.mandrill", "message_delayed", "jsonschema", "1-0-1").toSchemaUri,
    "spam" -> SchemaKey("com.mandrill", "message_marked_as_spam", "jsonschema", "1-0-1").toSchemaUri,
    "open" -> SchemaKey("com.mandrill", "message_opened", "jsonschema", "1-0-1").toSchemaUri,
    "reject" -> SchemaKey("com.mandrill", "message_rejected", "jsonschema", "1-0-0").toSchemaUri,
    "send" -> SchemaKey("com.mandrill", "message_sent", "jsonschema", "1-0-0").toSchemaUri,
    "soft_bounce" -> SchemaKey("com.mandrill", "message_soft_bounced", "jsonschema", "1-0-1").toSchemaUri,
    "unsub" -> SchemaKey("com.mandrill", "recipient_unsubscribed", "jsonschema", "1-0-1").toSchemaUri
  )

  /**
   * Converts a CollectorPayload instance into raw events.
   * A Mandrill Tracking payload contains many events in the body of the payload, stored within a
   * HTTP encoded string.
   * We expect the event parameter of these events to be 1 of 9 options otherwise we have an
   * unsupported event type.
   * @param payload The CollectorPayload containing one or more raw events as collected by a
   * Snowplow collector
   * @param resolver (implicit) The Iglu resolver used for schema lookup and validation. Not used
   * @return a Validation boxing either a NEL of RawEvents on Success, or a NEL of Failure Strings
   */
  def toRawEvents(payload: CollectorPayload)(
    implicit resolver: Resolver
  ): ValidatedNel[String, NonEmptyList[RawEvent]] =
    (payload.body, payload.contentType) match {
      case (None, _) => s"Request body is empty: no $VendorName events to process".invalidNel
      case (_, None) =>
        s"Request body provided but content type empty, expected $ContentType for $VendorName".invalidNel
      case (_, Some(ct)) if ct != ContentType =>
        s"Content type of $ct provided, expected $ContentType for $VendorName".invalidNel
      case (Some(body), _) =>
        payloadBodyToEvents(body) match {
          case Left(str) => str.invalidNel
          case Right(list) =>
            // Create our list of Validated RawEvents
            val rawEventsList: List[ValidatedNel[String, RawEvent]] =
              for {
                (event, index) <- list.zipWithIndex
              } yield {
                val eventOpt = event.hcursor.get[String]("event").toOption
                for {
                  schema <- lookupSchema(eventOpt, VendorName, index, EventSchemaMap).toValidatedNel
                } yield {
                  val formattedEvent =
                    cleanupJsonEventValues(event, eventOpt.map(("event", _)), List("ts"))
                  val qsParams = toMap(payload.querystring)
                  RawEvent(
                    api = payload.api,
                    parameters = toUnstructEventParams(
                      TrackerVersion,
                      qsParams,
                      schema,
                      formattedEvent,
                      "srv"),
                    contentType = payload.contentType,
                    source = payload.source,
                    context = payload.context
                  )
                }
              }
            // Processes the List for Failures and Successes and returns ValidatedRawEvents
            rawEventsListProcessor(rawEventsList)
        }
    }

  /**
   * Returns a list of events from the payload body of a Mandrill Event. Each event will be
   * formatted as an individual JSON.
   * NOTE: The payload.body string must adhere to UTF-8 encoding standards.
   * @param rawEventString The encoded string from the Mandrill payload body
   * @return a list of single events formatted as JSONs or a Failure String
   */
  private[registry] def payloadBodyToEvents(
    rawEventString: String
  ): Either[String, List[Json]] = {
    val bodyMap = toMap(
      URLEncodedUtils.parse(URI.create("http://localhost/?" + rawEventString), UTF_8).toList)
    bodyMap match {
      case map if map.size != 1 =>
        s"Mapped $VendorName body has invalid count of keys: ${map.size}".asLeft
      case map =>
        map.get("mandrill_events") match {
          case None => s"Mapped $VendorName body does not have 'mandrill_events' as a key".asLeft
          case Some("") => s"$VendorName events string is empty: nothing to process".asLeft
          case Some(dStr) =>
            parse(dStr) match {
              case Right(json) =>
                json.asArray match {
                  case Some(array) => array.toList.asRight
                  case _ => s"Could not resolve $VendorName payload into a JSON array".asLeft
                }
              case Left(e) =>
                s"$VendorName events couldn't be parsed as JSON: [${e.getMessage}]".asLeft
            }
        }
    }
  }

}
