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
package com.snowplowanalytics.snowplow.enrich.stream
package sinks

import java.nio.charset.StandardCharsets.UTF_8

import scala.collection.JavaConverters._

import com.snowplowanalytics.client.nsq.NSQProducer

import model._

/**
  * NSQSink for Scala enrichment
  */
class NsqSink(
  nsqConfig: NsqConfig,
  topicName: String
) extends Sink {

  private val producer = new NSQProducer().addAddress(nsqConfig.host, nsqConfig.port).start()

  /**
    *
    * @param events Sequence of enriched events and (unused) partition keys
    * @return Whether to checkpoint
    */
  override def storeEnrichedEvents(events: List[(String, String)]): Boolean = {
    val msgList = events.unzip._1.map(_.getBytes(UTF_8)).asJava
    producer.produceMulti(topicName, msgList)
    !events.isEmpty
  }

  override def flush(): Unit = ()
}
