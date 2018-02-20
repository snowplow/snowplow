/*
* Copyright (c) 2013-2017 Snowplow Analytics Ltd.
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
package com.snowplowanalytics.snowplow
package collectors
package scalastream
package sinks

// Scala
import scala.collection.JavaConverters._

// NSQ
import com.snowplowanalytics.client.nsq.NSQProducer

// This project
import model._

/**
  * NSQ Sink for the Scala collector
  *
  * @param nsqConfig Configuration for Nsq
  * @param topicName Nsq topic name
  */
class NsqSink(
  nsqConfig: Nsq,
  topicName: String
) extends Sink {

  override val MaxBytes = Long.MaxValue

  private val producer = new NSQProducer().addAddress(nsqConfig.host, nsqConfig.port).start()

  /**
   * Store raw events to the topic
   *
   * @param events The list of events to send
   * @param key The partition key (unused)
   */
  override def storeRawEvents(events: List[Array[Byte]], key: String): List[Array[Byte]] = {
    producer.produceMulti(topicName, events.asJava)
    Nil
  }
}
