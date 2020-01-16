/*
 * Copyright (c) 2012-2020 Snowplow Analytics Ltd. All rights reserved.
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

import com.snowplowanalytics.snowplow.badrows.Payload.{RawEvent => RE}

import loaders.CollectorPayload

/**
 * The canonical input format for the ETL process:
 * it should be possible to convert any collector payload to this raw event format via an `Adapter`,
 * ready for the main, collector-agnostic stage of the Enrichment.
 * Unlike `CollectorPayload`, where `body` can contain full POST payload with multiple events,
 * [[RawEvent]] is always a single Snowplow event
 */
final case class RawEvent(
  api: CollectorPayload.Api,
  parameters: RawEventParameters,
  contentType: Option[String], // Not yet used but should be logged
  source: CollectorPayload.Source,
  context: CollectorPayload.Context
)

object RawEvent {
  def toRawEvent(re: RawEvent): RE =
    RE(
      re.api.vendor,
      re.api.version,
      re.parameters,
      re.contentType,
      re.source.name,
      re.source.encoding,
      re.source.hostname,
      re.context.timestamp,
      re.context.ipAddress,
      re.context.useragent,
      re.context.refererUri,
      re.context.headers,
      re.context.userId
    )
}
