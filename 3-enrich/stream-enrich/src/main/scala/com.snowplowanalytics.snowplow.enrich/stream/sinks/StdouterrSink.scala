/*
 * Copyright (c) 2013-2014 Snowplow Analytics Ltd.
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
package com.snowplowanalytics.snowplow.enrich.stream
package sinks

// Snowplow
import com.snowplowanalytics.snowplow.collectors.thrift._

/**
 * Stdouterr Sink for Scala enrichment
 */
class StdouterrSink(inputType: InputType.InputType) extends ISink {

  /**
   * Side-effecting function to store the EnrichedEvent
   * to the given output stream.
   *
   * EnrichedEvent takes the form of a tab-delimited
   * String until such time as https://github.com/snowplow/snowplow/issues/211
   * is implemented.
   *
   * @param events Sequence of enriched events and (unused) partition keys
   * @return Whether to checkpoint
   */
  def storeEnrichedEvents(events: List[(String, String)]): Boolean = {
    inputType match {
      case InputType.Good => events.foreach(e => println(e._1)) // To stdout
      case InputType.Bad => events.foreach(e => Console.err.println(e._1)) // To stderr
    }
    !events.isEmpty
  }

  def flush() = ()
}
