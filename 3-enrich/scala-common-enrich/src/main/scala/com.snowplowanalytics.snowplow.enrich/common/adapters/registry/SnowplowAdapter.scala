/*
 * Copyright (c) 2012-2014 Snowplow Analytics Ltd. All rights reserved.
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

// Scalaz
import scalaz._
import Scalaz._

// This project
import loaders.CollectorPayload

/**
 * Transforms a collector payload which conforms to
 * a known version of the Snowplow Tracker Protocol
 * into raw events.
 */
object SnowplowAdapter {

  /**
   * Version 1 of the Tracker Protocol is GET only.
   * All data comes in on the querystring.
   */
  object Tp1 extends Adapter {

    /**
     * Converts a CollectorPayload instance into raw events.
     *
     * @param payload The CollectorPaylod containing one or more
     *        raw events as collected by a Snowplow collector
     * @return a Validation boxing either a NEL of RawEvents on
     *         Success, or a NEL of Failure Strings
     */
    def toRawEvents(payload: CollectorPayload): ValidatedRawEvents = {

      val params = toMap(payload.querystring)
      if (params.isEmpty) {
        "Querystring is empty: no raw event to process".failNel
      } else {
        NonEmptyList(RawEvent(
          vendor       = payload.vendor,
          version      = payload.version,
          parameters   = params,
          contentType  = payload.contentType,
          source       = payload.source,
          context      = payload.context
          )).success
      }
    }
  }

  /**
   * Version 2 of the Tracker Protocol supports GET and POST. Note that
   * with POST, data can still be passed on the querystring.
   */
  object Tp2 extends Adapter {

    /**
     * Converts a CollectorPayload instance into raw events.
     *
     * @param payload The CollectorPaylod containing one or more
     *        raw events as collected by a Snowplow collector
     * @return a Validation boxing either a NEL of RawEvents on
     *         Success, or a NEL of Failure Strings
     */
    def toRawEvents(payload: CollectorPayload): ValidatedRawEvents = {
      
      // TODO:
      // 1. Validate the JSON
      // 2. Convert the JSON to a map
      // 3. Merge the querystring into the map (qs should take priority)
      // 4. Complain if final map is empty

      val qsParams = toMap(payload.querystring)

      // Parameters in the querystring take priority, i.e.
      // override the same parameter in the POST body
      val allParams = qsParams // TODO: fix this

      if (allParams.isEmpty) {
        "No parameters found for this raw event".failNel
      } else {
        NonEmptyList(RawEvent(
          vendor       = payload.vendor,
          version      = payload.version,
          parameters   = allParams,
          contentType  = payload.contentType,
          source       = payload.source,
          context      = payload.context
          )).success
      }

    }
  }

}
