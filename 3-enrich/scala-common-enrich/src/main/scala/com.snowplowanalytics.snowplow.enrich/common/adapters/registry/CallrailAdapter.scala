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

import cats.Monad
import cats.data.{NonEmptyList, ValidatedNel}
import cats.effect.Clock
import cats.syntax.validated._
import com.snowplowanalytics.iglu.client.Client
import com.snowplowanalytics.iglu.client.resolver.registries.RegistryLookup
import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer}
import io.circe.Json
import org.joda.time.DateTimeZone
import org.joda.time.format.DateTimeFormat

import loaders.CollectorPayload
import utils._

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
    val CallComplete = SchemaKey(
      "com.callrail",
      "call_complete",
      "jsonschema",
      SchemaVer.Full(1, 0, 2)
    ).toSchemaUri
  }

  // Datetime format used by CallRail (as we will need to massage)
  private val CallrailDateTimeFormat =
    DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").withZone(DateTimeZone.UTC)

  // Create a simple formatter function
  private val CallrailFormatter: FormatterFunc = {
    val bools = List("first_call", "answered")
    val ints = List("duration")
    val dateTimes: JsonUtils.DateTimeFields =
      Some((NonEmptyList.of("datetime"), CallrailDateTimeFormat))
    buildFormatter(bools, ints, dateTimes)
  }

  /**
   * Converts a CollectorPayload instance into raw events. A CallRail payload only contains a single
   * event.
   * @param payload The CollectorPaylod containing one or more raw events
   * @param client The Iglu client used for schema lookup and validation
   * @return a Validation boxing either a NEL of RawEvents on Success, or a NEL of Failure Strings
   */
  override def toRawEvents[F[_]: Monad: RegistryLookup: Clock](
    payload: CollectorPayload,
    client: Client[F, Json]
  ): F[ValidatedNel[String, NonEmptyList[RawEvent]]] = {
    val _ = client
    val params = toMap(payload.querystring)
    if (params.isEmpty) {
      Monad[F].pure("Querystring is empty: no CallRail event to process".invalidNel)
    } else {
      Monad[F].pure(
        NonEmptyList
          .of(
            RawEvent(
              api = payload.api,
              parameters = toUnstructEventParams(
                TrackerVersion,
                params,
                SchemaUris.CallComplete,
                CallrailFormatter,
                "srv"
              ),
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
