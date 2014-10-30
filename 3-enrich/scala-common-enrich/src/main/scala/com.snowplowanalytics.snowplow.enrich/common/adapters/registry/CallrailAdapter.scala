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
  private val JsonSchemaDateTimeFormat = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").withZone(DateTimeZone.UTC)

  // Fields known to need datatype cleaning/conversion ready for JSON Schema validation
  private val BoolFields = List("first_call", "answered")
  private val IntFields  = List("duration")
  private val DateFields = Some(NonEmptyList("datetime"), CallrailDateTimeFormat)

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
   * Converts a boolean-like String of value "true"
   * or "false" to a JBool value of true or false
   * respectively. Any other value becomes a
   * JString.
   *
   * No erroring if the String is not boolean-like,
   * leave it to eventual JSON Schema validation
   * to enforce that. 
   *
   * @param str The boolean-like String to convert
   * @return true for "true", false for "false",
   *         and otherwise a JString wrapping the
   *         original String
   */
  def booleanToJValue(str: String): JValue = str match {
    case "true" => JBool(true)
    case "false" => JBool(false)
    case _ => JString(str)
  }

  /**
   * Converts an integer-like String to a
   * JInt value. Any other value becomes a
   * JString.
   *
   * No erroring if the String is not integer-like,
   * leave it to eventual JSON Schema validation
   * to enforce that.
   *
   * @param str The integer-like String to convert
   * @return a JInt if the String was integer-like,
   *         or else a JString wrapping the original
   *         String.
   */
  def integerToJValue(str: String): JValue =
    try {
      JInt(new JBigInteger(str))
    } catch {
      case nfe: NumberFormatException =>
        JString(str)
    }

  /**
   * Reformats a non-standard date-time into a format
   * compatible with JSON Schema's date-time format
   * validation. If the String does not match the
   * expected date format, then return the original String.
   *
   * @param str The date-time-like String to reformat
   *        to pass JSON Schema validation
   * @return a JString, of the reformatted date-time if
   *         possible, or otherwise the original String
   */
  def reformatDateTime(str: String, fromFormat: DateTimeFormatter): JString =
    JString(try {
      val dt = DateTime.parse(str, fromFormat)
      toJsonSchemaDateTime(dt)
    } catch {
      case iae: IllegalArgumentException => str
    })

  /**
   * Converts an incoming key, value into a json4s JValue.
   * Uses the lists of keys which should contain bools,
   * ints and dates to apply specific processing to
   * those values when found.
   *
   * @param key The key of the field to generate. Also used
   *        to determine what additional processing should
   *        be applied to the value
   * @param value The value of the field
   * @param bools A List of keys whose values should be
   *        processed as boolean-like Strings
   * @param ints A List of keys whose values should be
   *        processed as integer-like Strings
   * @param dates If Some, a NEL of keys whose values should
   *        be treated as date-time-like Strings, which will
   *        require processing from the specified format
   * @return a JField, containing the original key and the
   *         processed String, now as a JValue
   */
  def toJField(key: String, value: String, bools: List[String], ints: List[String],
    dates: Option[Tuple2[NonEmptyList[String], DateTimeFormatter]]): JField = {
    
    val v = (value, dates) match {
      case ("", _)                  => JNull
      case _ if bools.contains(key) => booleanToJValue(value)
      case _ if ints.contains(key)  => integerToJValue(value)
      case (_, Some((nel, fmt)))
        if nel.toList.contains(key) => reformatDateTime(value, fmt)
      case _                        => JString(value)
    }
    (key, v)
  }

}
