/*
 * Copyright (c) 2012-2019 Snowplow Analytics Ltd. All rights reserved.
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
package snowplow

import com.snowplowanalytics.iglu.client.Resolver
import scalaz._
import Scalaz._

import loaders.CollectorPayload

/**
 * Version 1 of the Tracker Protocol is GET only.
 * All data comes in on the querystring.
 */
object Tp1Adapter extends Adapter {

  /**
   * Converts a CollectorPayload instance into raw events.
   * Tracker Protocol 1 only supports a single event in a
   * payload.
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
      "Querystring is empty: no raw event to process".failNel
    } else {
      NonEmptyList(
        RawEvent(
          api = payload.api,
          parameters = params,
          contentType = payload.contentType,
          source = payload.source,
          context = payload.context
        )).success
    }
  }
}
