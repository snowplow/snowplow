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

import cats.Monad
import cats.data.{NonEmptyList, ValidatedNel}
import cats.effect.Clock
import cats.syntax.validated._
import com.snowplowanalytics.iglu.client.Client
import com.snowplowanalytics.iglu.client.resolver.registries.RegistryLookup
import io.circe.Json

import loaders.CollectorPayload
import outputs._

/** Version 1 of the Tracker Protocol is GET only. All data comes in on the querystring. */
object Tp1Adapter extends Adapter {

  /**
   * Converts a CollectorPayload instance into raw events. Tracker Protocol 1 only supports a single
   * event in a payload.
   * @param payload The CollectorPaylod containing one or more raw events
   * @param client The Iglu client used for schema lookup and validation
   * @return a Validation boxing either a NEL of RawEvents on Success, or a NEL of Failure Strings
   */
  override def toRawEvents[F[_]: Monad: RegistryLookup: Clock](
    payload: CollectorPayload,
    client: Client[F, Json]
  ): F[ValidatedNel[AdapterFailure, NonEmptyList[RawEvent]]] = {
    val _ = client
    val params = toMap(payload.querystring)
    if (params.isEmpty) {
      val msg = "empty querystring: not a valid URI redirect"
      val failure = InputDataAdapterFailure("querystring", None, msg)
      Monad[F].pure(failure.invalidNel)
    } else {
      Monad[F].pure(
        NonEmptyList
          .one(
            RawEvent(
              api = payload.api,
              parameters = params,
              contentType = payload.contentType,
              source = payload.source,
              context = payload.context
            )
          )
          .valid
      )
    }
  }
}
