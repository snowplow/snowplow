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

// Joda-Time
import org.joda.time.DateTime

// This project
import loaders.{
  CollectorSource,
  CollectorContext,
  CollectorApi
}

/**
 * The canonical input format for the ETL
 * process: it should be possible to
 * convert any collector payload to this
 * raw event format via an _adapter_,
 * ready for the main, collector-agnostic
 * stage of the Enrichment.
 */
final case class RawEvent(
  api:         CollectorApi,
  parameters:  RawEventParameters,
  contentType: Option[String], // Not yet used but should be logged
  source:      CollectorSource,
  context:     CollectorContext
  )
