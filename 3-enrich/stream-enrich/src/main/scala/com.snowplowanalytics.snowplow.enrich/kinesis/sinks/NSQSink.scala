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
package com.snowplowanalytics.snowplow.enrich.kinesis
package sinks

// Java
import java.nio.charset.StandardCharsets.UTF_8

// NSQ
import com.github.brainlag.nsq.NSQProducer


/**
  * Stdouterr Sink for Scala enrichment
  */
class NSQSink(config: KinesisEnrichConfig, inputType: InputType.InputType) extends ISink {

  val producer = new NSQProducer().addAddress(config.nsqdHost, config.nsqdPort).start();

  val topicName = inputType match {
    case InputType.Good =>config.nsqGoodSinkTopicName
    case InputType.Bad => config.nsqBadTopicName
  }

  /**
    *
    * @param events Sequence of enriched events and (unused) partition keys
    * @return Whether to checkpoint
    */

  def storeEnrichedEvents(events: List[(String, String)]): Boolean = {
    
    events foreach{
      e => producer.produce(topicName, e._1.getBytes(UTF_8))
    }
    !events.isEmpty
  }

  def flush() = ()
}
