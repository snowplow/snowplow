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

// Iglu
import iglu.client.{
  SchemaKey,
  Resolver
}

// Scalaz
import scalaz._
import Scalaz._

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
object AdXTrackingAdapter extends Adapter {

  // Tracker version for an AD-X Tracking webhook
  private val TrackerVersion = "com.adxtracking-v1"

  // Schemas for reverse-engineering a Snowplow unstructured event
  private object SchemaUris {
    val UnstructEvent = SchemaKey("com.snowplowanalytics.snowplow", "unstruct_event", "jsonschema", "1-0-0").toSchemaUri
    val AppInstall = SchemaKey("com.adxtracking", "app_install", "jsonschema", "1-0-0").toSchemaUri
  }

  /**
   * Converts a CollectorPayload instance into raw events.
   * An AD-X Tracking payload only contains a single event.
   * We expect the name parameter to be "Install", otherwise
   * we have an unsupported event type.
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
      "Querystring is empty: no AD-X Tracking event to process".failNel
    } else {
      params.get("name") match {
        case None => "Querystring does not contain name parameter: cannot determine type of AD-X Tracking event".failNel
        case Some(name) if name != "Install" => s"Unexpected name parameter ${name} for AD-X Tracking event; expected Install".failNel
        case Some(_) =>
          NonEmptyList(RawEvent(
            api          = payload.api,
            parameters   = toUnstructEventParams(TrackerVersion, params - "name"),
            contentType  = payload.contentType,
            source       = payload.source,
            context      = payload.context
            )).success
      }
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
  private def toUnstructEventParams(tracker: String, parameters: RawEventParameters):
    RawEventParameters = {

    val params = compact {
      ("schema" -> SchemaUris.UnstructEvent) ~
      ("data"   -> (
        ("schema" -> SchemaUris.AppInstall) ~
        ("data"   -> (parameters -("nuid", "aid", "cv", "p")))
      ))
    }

    Map(
      "tv"    -> tracker,
      "e"     -> "ue",
      "p"     -> parameters.getOrElse("p", "app"), // Required field
      "ue_pr" -> params) ++
    parameters.filterKeys(Set("nuid", "aid", "cv"))
  }
}
