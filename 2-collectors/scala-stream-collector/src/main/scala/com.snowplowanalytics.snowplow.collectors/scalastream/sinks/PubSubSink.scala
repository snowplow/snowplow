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
import com.google.cloud.ByteArray
import com.google.cloud.pubsub.{PubSub, PubSubOptions, Topic, TopicInfo, Message}
import com.google.auth.oauth2.GoogleCredentials

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

  private val pubSubTopic = getTopicObject

  /**
   * Instantiates an existing Topic on Cloud Pub/Sub,
   * with the given configuration options. If the name isn't correct, this will fail
   *
   * @return a PubSub Topic object
   */
  private def getTopicObject: Topic = {
    val pubsubOptions = if (config.googleAuthPath == "env") {
        PubSubOptions.getDefaultInstance()
    } else {
        val optBuilder = PubSubOptions.newBuilder()
        optBuilder.setProjectId(config.googleProjectId)
        optBuilder.setCredentials(GoogleCredentials.fromStream(new FileInputStream(config.googleAuthPath)))
        optBuilder.build()
    }
    val pubsub = pubsubOptions.getService()
    pubsub.getTopic(topicName)
  }

  /**
   * Store raw events to the topic
   *
   * @param events The list of events to send
   * @param key Not used.
   */
  override def storeRawEvents(events: List[Array[Byte]], key: String) = {
    debug(s"Writing ${events.size} Thrift records to PubSub topic ${topicName}")
    val messages = events.map(event => Message.of(ByteArray.copyFrom(event)))
    if (messages.size != 0) {
      try {
          pubSubTopic.publish(messages)
      } catch {
        case e: Exception => {
          error(s"Unable to send events: ${e.getMessage}")
          e.printStackTrace()
        }
      }
      Nil
    } else Nil
  }

  override def getType = Sink.PubSub
}
