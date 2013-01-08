/*
 * Copyright (c) 2012 SnowPlow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.hadoop.etl
package loaders

// Joda-Time
import org.joda.time.{DateTime => JoDateTime}

/**
 * All payloads sent by trackers must inherit from
 * this class.
 */
trait TrackerPayload

/**
 * A tracker payload for a single event, delivered
 * via the querystring on a GET.
 */
case class GetPayload(val payload: String) extends TrackerPayload

/**
 * The canonical input format for the ETL
 * process: it should be possible to
 * convert any collector input format to
 * this format, ready for the main,
 * collector-agnostic stage of the ETL.
 */
final case class CanonicalInput(
  val timestamp:  JoDateTime,
  val payload:    TrackerPayload,
  val ipAddress:  Option[String],
  val userAgent:  Option[String],
  val refererUri: Option[String],
  val userId:     Option[String]
  )