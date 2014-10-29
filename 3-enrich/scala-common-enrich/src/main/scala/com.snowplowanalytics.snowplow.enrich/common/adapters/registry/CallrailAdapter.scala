/*
 * Copyright (c) 2014 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics
package snowplow
package enrich
package common
package adapters
package registry

// Java
import java.math.{BigInteger => JBigInteger}

// Iglu
import iglu.client.{
  SchemaKey,
  Resolver
}

// Scalaz
import scalaz._
import Scalaz._

// Joda-Time
import org.joda.time.{DateTime, DateTimeZone}
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}

// json4s
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

// This project
import loaders.CollectorPayload
import utils.JsonUtils

/**
 * Transforms a collector payload which conforms to
 * a known version of the AD-X Tracking webhook
 * into raw events.
 */
object CallrailAdapter extends Adapter {

  // Tracker version for an AD-X Tracking webhook
  private val TrackerVersion = "com.callrail-v1"

  // Schemas for reverse-engineering a Snowplow unstructured event
  private object SchemaUris {
    val UnstructEvent = SchemaKey("com.snowplowanalytics.snowplow", "unstruct_event", "jsonschema", "1-0-0").toSchemaUri
    val CallComplete = SchemaKey("com.callrail", "call_complete", "jsonschema", "1-0-0").toSchemaUri
  }

  // Datetime format used by CallRail (as we will need to massage)
  private val CallrailDateTimeFormat = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").withZone(DateTimeZone.UTC)

  // TODO: move this out
  private val JsonSchemaDateTimeFormat = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZ").withZone(DateTimeZone.UTC)

  // Fields known to need datatype cleaning/conversion ready for JSON Schema validation
  private val BoolFields = List("first_call", "answered")
  private val IntFields  = List("duration")
  private val DateFields = List("datetime")

  /**
   * Converts a Joda DateTime into
   * a JSON Schema-compatible date-time string.
   *
   * @param datetime The Joda DateTime
   *        to convert to a timestamp String
   * @return the timestamp String
   */
  def toJsonSchemaDateTime(dateTime: DateTime): String = JsonSchemaDateTimeFormat.print(dateTime)

  /**
   * Converts a CollectorPayload instance into raw events.
   * A CallRail payload only contains a single event.
   *
   * @param payload The CollectorPaylod containing one or more
   *        raw events as collected by a Snowplow collector
   * @param resolver (implicit) The Iglu resolver used for
   *        schema lookup and validation. Not used
   * @return a Validation boxing either a NEL of RawEvents on
   *         Success, or a NEL of Failure Strings
   */
  def toRawEvents(payload: CollectorPayload)(implicit resolver: Resolver): ValidatedRawEvents = {

    val params = toMap(payload.querystring)
    if (params.isEmpty) {
      "Querystring is empty: no CallRail event to process".failNel
    } else {
      NonEmptyList(RawEvent(
        api          = payload.api,
        parameters   = toUnstructEventParams(TrackerVersion, params),
        contentType  = payload.contentType,
        source       = payload.source,
        context      = payload.context
        )).success
    }
  }

  /**
   * Fabricates a Snowplow unstructured event from
   * the supplied parameters. Note that to be a
   * valid Snowplow unstructured event, the event
   * must contain e, p and tv parameters, so we
   * make sure to set those.
   *
   * @param tracker The name and version of this
   *        tracker
   * @param parameters The raw-event parameters
   *        we will nest into the unstructured event
   * @return the raw-event parameters for a valid
   *         Snowplow unstructured event
   */
  // TODO: move this out and standardize
  private def toUnstructEventParams(tracker: String, parameters: RawEventParameters):
    RawEventParameters = {

    val params: JObject = for {
      p <- (parameters -("nuid", "aid", "cv", "p")).toList
    } yield toJField(p._1, p._2, BoolFields, IntFields, DateFields)

    val json = compact {
      ("schema" -> SchemaUris.UnstructEvent) ~
      ("data"   -> (
        ("schema" -> SchemaUris.CallComplete) ~
        ("data"   -> params)
      ))
    }

    Map(
      "tv"    -> tracker,
      "e"     -> "ue",
      "p"     -> parameters.getOrElse("p", "app"), // Required field
      "ue_pr" -> json) ++
    parameters.filterKeys(Set("nuid", "aid", "cv"))
  }

  /**
   * Converts a String of value "true" or "false"
   * to true or false respectively. Any other
   * value is passed through intact. This makes
   * this adapter less brittle and stops us from
   * validating in two places (once in the adapter
   * and a second time in the shreder).
   *
   * @param str The String to convert
   * @return true for "true", false for "false",
   *         null for "" and otherwise the original
   *         String
   */
  def toMaybeJBool(str: String): JValue = str match {
    case "true" => JBool(true)
    case "false" => JBool(false)
    case _ => JString(str)
  }

  def toMaybeJInt(str: String): JValue =
    try {
      JInt(new JBigInteger(str))
    } catch {
      case nfe: NumberFormatException =>
        JString(str)
    }

  def fixDateTime(str: String, fromFormat: DateTimeFormatter): JValue =
    try {
      val dt = DateTime.parse(str, fromFormat)
      JString(toJsonSchemaDateTime(dt))
    } catch {
      case iae: IllegalArgumentException => {
        JString(str)
      }
    }

  def toJField(key: String, value: String, bools: List[String], ints: List[String], dates: List[String]): JField = {
    val v = value match {
      case ""                       => JNull
      case _ if bools.contains(key) => toMaybeJBool(value)
      case _ if ints.contains(key)  => toMaybeJInt(value)
      case _ if dates.contains(key) => fixDateTime(value, CallrailDateTimeFormat) // TODO: make this pluggable
      case _                        => JString(value)
    }
    (key, v)
  }

}
