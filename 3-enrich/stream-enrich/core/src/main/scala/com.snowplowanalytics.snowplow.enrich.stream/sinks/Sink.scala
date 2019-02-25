/*
 * Copyright (c) 2013-2019 Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache
 * License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at
 * http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied.
 *
 * See the Apache License Version 2.0 for the specific language
 * governing permissions and limitations there under.
 */
package com.snowplowanalytics.snowplow.enrich
package stream.sinks

import org.slf4j.LoggerFactory

/**
 * An interface for all sinks to use to store events.
 */
trait Sink {

  lazy val log = LoggerFactory.getLogger(getClass())

  /** Side-effecting function to store the EnrichedEvent to the given output stream. */
  def storeEnrichedEvents(events: List[(String, String)]): Boolean

  def flush(): Unit
}
