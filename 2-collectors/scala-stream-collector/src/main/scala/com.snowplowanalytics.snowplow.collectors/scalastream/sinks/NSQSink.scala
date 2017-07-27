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
package com.snowplowanalytics.snowplow
package collectors
package scalastream
package sinks

//NSQ
import com.github.brainlag.nsq.NSQProducer

// Apache Commons
import org.apache.commons.codec.binary.Base64

class NSQSink (config: CollectorConfig, inputType: InputType.InputType) extends AbstractSink {

  val MaxBytes = Long.MaxValue

  private val topicName = inputType match {
    case InputType.Good => config.nsqTopicGoodName
    case InputType.Bad  => config.nsqTopicBadName
  }

  val producer = new NSQProducer().addAddress(config.nsqdHost, config.nsqdPort).start();

  // Send a Base64-encoded event to .
  def storeRawEvents(events: List[Array[Byte]], key: String) = {

    events foreach {
      e => producer.produce(topicName, Base64.encodeBase64(e))
    }

    Nil
  }

  override def getType = Sink.NSQ
}
