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

    // TODO: add code which complains if querystring is empty.

    def toRawEvents(payload: CollectorPayload): ValidatedRawEvents =
      List(RawEvent(
        timestamp    = payload.timestamp,
        vendor       = payload.vendor,
        version      = payload.version,
        parameters   = toMap(payload.querystring),
        source       = payload.source,
        encoding     = payload.encoding,
        ipAddress    = payload.ipAddress,
        userAgent    = payload.userAgent,
        refererUri   = payload.refererUri,
        headers      = payload.headers,
        userId       = payload.userId
        )).success
  }

  /**
   * Version 2 of the Tracker Protocol supports GET and POST. Note that
   * with POST, data can still be passed on the querystring.
   */
  object Tp2 extends Adapter {

    def toRawEvents(payload: CollectorPayload): ValidatedRawEvents = {
      
      // TODO:
      // 1. Validate the JSON
      // 2. Convert the JSON to a map
      // 3. Merge the querystring into the map (qs should take priority)
      // 4. Complain if final map is empty

      // Placeholder for now
      List(RawEvent(
        timestamp    = payload.timestamp,
        vendor       = payload.vendor,
        version      = payload.version,
        parameters   = toMap(payload.querystring),
        source       = payload.source,
        encoding     = payload.encoding,
        ipAddress    = payload.ipAddress,
        userAgent    = payload.userAgent,
        refererUri   = payload.refererUri,
        headers      = payload.headers,
        userId       = payload.userId
        )).success
    }
  }

}
