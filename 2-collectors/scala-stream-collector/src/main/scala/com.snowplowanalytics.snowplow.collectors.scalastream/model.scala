/* 
 * Copyright (c) 2015 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0, and
 * you may not use this file except in compliance with the Apache License
 * Version 2.0.  You may obtain a copy of the Apache License Version 2.0 at
 * http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the Apache License Version 2.0 is distributed on an "AS
 * IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the Apache License Version 2.0 for the specific language
 * governing permissions and limitations there under.
 */
package com.snowplowanalytics.snowplow.collectors.scalastream

import sinks._

package model {

  /** Whether the sink is for good rows or bad rows */
  object InputType extends Enumeration {
    type InputType = Value
    val Good, Bad = Value
  }

  /** Type of sink */
  object SinkType extends Enumeration {
    type Sink = Value
    val Kinesis, Kafka, Stdout, Test = Value
  }

  /**
   * Case class for holding both good and
   * bad sinks for the Stream Collector.
   *
   * @param good
   * @param bad
   */
  case class CollectorSinks(good: Sink, bad: Sink)

  /**
   * Case class for holding the results of
   * splitAndSerializePayload.
   *
   * @param good All good results
   * @param bad All bad results
   */
  case class EventSerializeResult(good: List[Array[Byte]], bad: List[Array[Byte]])

  /**
   * Class for the result of splitting a too-large array of events in the body of a POST request
   *
   * @param goodBatches List of batches of events
   * @param failedBigEvents List of events that were too large
   */
  case class SplitBatchResult(goodBatches: List[List[String]], failedBigEvents: List[String])   
}
