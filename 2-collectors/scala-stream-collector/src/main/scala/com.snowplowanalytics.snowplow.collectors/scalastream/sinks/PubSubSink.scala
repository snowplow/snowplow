/*
 * Copyright (c) 2013-2016 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.collectors
package scalastream
package sinks

// Java
import java.util.Properties
import java.io.FileInputStream

// Scala
import scala.collection.JavaConversions._

// PubSub
import com.google.cloud.pubsub.spi.v1.Publisher
import com.google.pubsub.v1.TopicName
import com.google.pubsub.v1.PubsubMessage
import com.google.protobuf.ByteString


// Config
import com.typesafe.config.Config

// Logging
import org.slf4j.LoggerFactory


/**
 * PubSub Sink for the Scala collector
 */
class PubSubSink(config: CollectorConfig, inputType: InputType.InputType) extends AbstractSink {

  import log.{error, debug, info, trace}

  // not used, but required for this class to implement AbstractSink
  val MaxBytes = 1000000L

  private val topicName = inputType match {
    case InputType.Good => config.pubsubTopicGoodName
    case InputType.Bad  => config.pubsubTopicBadName
  }

  private val pubSubPublisher = createPublisher

  /**
   * Instantiates a Publisher on an existing topic
   * with the given configuration options. If the name isn't correct, this will fail
   *
   * @return a PubSub Topic object
   */
  private def createPublisher: Publisher =
    Publisher.newBuilder(
        TopicName.create(s"${config.googleProjectId}",  s"$topicName")
    ).build

  /**
   * Convert event bytes to PubsubMessage to be published
   * @param event Event to be converted
   * @return PubsubMessage instance
   */
  private def eventToPubsubMessage(event: Array[Byte]): PubsubMessage = {
    PubsubMessage.newBuilder
      .setData(ByteString.copyFrom(event))
      .build
  }

  /**
   * Store raw events to the topic
   *
   * @param events The list of events to send
   * @param key Not used.
   */
  override def storeRawEvents(events: List[Array[Byte]], key: String) = {
    debug(s"Writing ${events.size} Thrift records to PubSub topic ${topicName}")
    events.foreach(event => pubSubPublisher.publish(eventToPubsubMessage(event)))
    Nil
  }

  override def getType = Sink.PubSub
}
