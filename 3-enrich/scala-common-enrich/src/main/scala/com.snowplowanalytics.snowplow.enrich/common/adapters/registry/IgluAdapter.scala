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
 * Transforms a collector payload which provides a set
 * of name-value pairs on a GET querystring, along
 * with a &schema=[[iglu schema uri]] parameter to
 * attribute the name-value pairs to an
 * Iglu-compatible self-describing JSON.
 */
object IgluAdapter extends Adapter {

  // Tracker version for an Iglu-compatible webhook
  private val TrackerVersion = "com.snowplowanalytics.iglu-v1"

  // Create a simple formatter function
  private val IgluFormatter: FormatterFunc = buildFormatter() // For defaults

  /**
   * Converts a CollectorPayload instance into raw events.
   * Currently we only support a single event Iglu-compatible
   * self-describing event passed in on the querystring.
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
      "Querystring is empty: no Iglu-compatible event to process".failNel
    } else {
      params.get("schema") match {
        case None => "Querystring does not contain schema parameter: not an Iglu-compatible self-describing event".failNel
        case Some(schemaUri) => SchemaKey.parse(schemaUri) match {
          case Failure(procMsg) => procMsg.getMessage.failNel
          case Success(_)       =>
            NonEmptyList(RawEvent(
              api          = payload.api,
              parameters   = toUnstructEventParams(TrackerVersion, (params - "schema"), schemaUri, IgluFormatter, "app"),
              contentType  = payload.contentType,
              source       = payload.source,
              context      = payload.context
              )).success
        }
      }
    }
  }
}
