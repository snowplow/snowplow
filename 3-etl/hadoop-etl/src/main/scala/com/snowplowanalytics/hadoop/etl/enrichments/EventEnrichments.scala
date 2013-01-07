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
package com.snowplowanalytics.snowplow.hadoop.etl.enrichments

// Java
import java.util.UUID

/**
 * Holds the enrichments related to events.
 */
object EventEnrichments {

  /**
   * Turns an event code into a valid event type,
   * e.g. "pv" -> "page_view"
   *
   * @param eventCode The event code
   * @return The event type (Option-boxed), or None
   */
  def asEventType(eventCode: String): Option[String] = eventCode match {
    case "ev" => Some("custom")
    case "ad" => Some("ad_impression")
    case "tr" => Some("transaction")
    case "ti" => Some("transaction_item")
    case "pv" => Some("page_view")
    case "pp" => Some("page_ping")
    case _    => None
  }

  /**
   * Returns a unique event ID. ID is 
   * generated as a type 4 UUID, then
   * converted to a String.
   *
   * @return The event ID
   */
  def generateEventId(): String = UUID.randomUUID().toString
}