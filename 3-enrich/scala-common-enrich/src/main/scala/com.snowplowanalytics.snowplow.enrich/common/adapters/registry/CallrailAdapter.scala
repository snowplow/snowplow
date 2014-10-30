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
import utils.{JsonUtils => JU}

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

  // Fields known to need datatype cleaning/conversion ready for JSON Schema validation
  private object Fields {
    val Bools = List("first_call", "answered")
    val Ints = List("duration")
    val DateTimes: JU.DateTimeFields = Some(NonEmptyList("datetime"), CallrailDateTimeFormat)
  }

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
        parameters   = toUnstructEventParams(TrackerVersion, params,
                         SchemaUris.CallComplete, Fields.Bools, Fields.Ints, Fields.DateTimes),
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
   * @param schema The schema key which defines this
   *        unstructured event as a String
   * @param bools A List of keys whose values should be
   *        processed as boolean-like Strings
   * @param ints A List of keys whose values should be
   *        processed as integer-like Strings
   * @param dates If Some, a NEL of keys whose values should
   *        be treated as date-time-like Strings, which will
   *        require processing from the specified format
   * @return the raw-event parameters for a valid
   *         Snowplow unstructured event
   */
  // TODO: move this out and standardize
  private def toUnstructEventParams(tracker: String, parameters: RawEventParameters, schema: String,
    bools: List[String], ints: List[String], dateTimes: JU.DateTimeFields): RawEventParameters = {

    val params: JObject = for {
      p <- (parameters -("nuid", "aid", "cv", "p")).toList
    } yield JU.toJField(p._1, p._2, bools, ints, dateTimes)

    val json = compact {
      ("schema" -> SchemaUris.UnstructEvent) ~
      ("data"   -> (
        ("schema" -> schema) ~
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

}
