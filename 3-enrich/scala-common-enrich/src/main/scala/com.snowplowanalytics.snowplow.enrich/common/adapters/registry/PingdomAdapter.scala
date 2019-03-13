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

import org.apache.http.NameValuePair

import cats.syntax.either._
import com.snowplowanalytics.iglu.client.{Resolver, SchemaKey}
import io.circe._
import scalaz._
import Scalaz._

import loaders.CollectorPayload

/**
 * Transforms a collector payload which conforms to a known version of the Pingdom Tracking webhook
 * into raw events.
 */
object PingdomAdapter extends Adapter {
  // Vendor name for Failure Message
  private val VendorName = "Pingdom"

  // Tracker version for an Pingdom Tracking webhook
  private val TrackerVersion = "com.pingdom-v1"

  // Regex for extracting data from querystring values which we
  // believe are incorrectly handled Python unicode strings.
  private val PingdomValueRegex = """\(u'(.+)',\)""".r

  // Schemas for reverse-engineering a Snowplow unstructured event
  private val EventSchemaMap = Map(
    "assign" -> SchemaKey("com.pingdom", "incident_assign", "jsonschema", "1-0-0").toSchemaUri,
    "notify_user" -> SchemaKey("com.pingdom", "incident_notify_user", "jsonschema", "1-0-0").toSchemaUri,
    "notify_of_close" -> SchemaKey("com.pingdom", "incident_notify_of_close", "jsonschema", "1-0-0").toSchemaUri
  )

  /**
   * Converts a CollectorPayload instance into raw events. A Pingdom Tracking payload only contains
   * a single event. We expect the name parameter to be one of two types otherwise we have an
   * unsupported event type.
   * @param payload The CollectorPayload containing one or more raw events
   * @param resolver (implicit) The Iglu resolver used for schema lookup and validation. Not used
   * @return a Validation boxing either a NEL of RawEvents on Success, or a NEL of Failure Strings
   */
  def toRawEvents(payload: CollectorPayload)(implicit resolver: Resolver): ValidatedRawEvents =
    (payload.querystring) match {
      case Nil => s"$VendorName payload querystring is empty: nothing to process".failNel
      case qs =>
        reformatMapParams(qs) match {
          case Failure(f) => f.fail
          case Success(s) =>
            s.get("message") match {
              case None =>
                s"$VendorName payload querystring does not have 'message' as a key".failNel
              case Some(event) =>
                for {
                  parsedEvent <- parseJsonSafe(event)
                  schema <- {
                    val eventOpt = parsedEvent.hcursor.downField("action").as[String].toOption
                    lookupSchema(eventOpt, VendorName, EventSchemaMap)
                  }
                } yield {
                  val formattedEvent = reformatParameters(parsedEvent)
                  val qsParams = s - "message"
                  NonEmptyList(
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
                  )
                }
            }
        }
    }

  /**
   * As Pingdom wraps each value in the querystring within: (u'[content]',) we need to check that
   * these wrappers have been removed by the Collector before we can use them.
   * example: p -> (u'app',) becomes p -> app
   * The expected behavior is that every value will have been cleaned by the Collector; however if
   * this is not the case we will not be able to process values.
   * @param params A list of name-value pairs from the querystring of the Pingdom payload
   * @return a Map of name-value pairs which has been validated as all passing the regex extraction
   * or return a NonEmptyList of Failures if any could not pass the regex.
   */
  private[registry] def reformatMapParams(
    params: List[NameValuePair]
  ): Validated[Map[String, String]] = {
    val formatted = params.map { value =>
      (value.getName, value.getValue) match {
        case (k, PingdomValueRegex(v)) =>
          (s"$VendorName name-value pair [$k -> $v]: Passed regex - Collector is not catching " +
            "unicode wrappers anymore").failNel
        case (k, v) => (k -> v).successNel
      }
    }

    val successes: List[(String, String)] =
      for {
        Success(s) <- formatted
      } yield s

    val failures: List[String] =
      for {
        Failure(NonEmptyList(f)) <- formatted
      } yield f

    (successes, failures) match {
      case (s :: ss, Nil) => (s :: ss).toMap.successNel // No Failures collected.
      case (_, f :: fs) => NonEmptyList(f, fs: _*).fail // Some Failures, return only those.
      case (Nil, Nil) =>
        ("Empty parameters list was passed - should never happen: empty " +
          "querystring is not being caught").failNel
    }
  }

  /**
   * Returns an updated Pingdom Event JSON where the "action" field has been removed
   * @param json The event JSON which we need to update values for
   * @return the updated JSON without the "action" field included
   */
  private[registry] def reformatParameters(json: Json): Json =
    json.hcursor.downField("action").delete.top.getOrElse(json)
}
