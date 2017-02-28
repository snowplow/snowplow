/*
 * Copyright (c) 2013-2016 Snowplow Analytics Ltd.
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
package kinesis
package sinks

// Java
import java.util.Properties

// Pubsub
import com.google.cloud.pubsub.spi.v1.Publisher
import com.google.pubsub.v1.TopicName
import com.google.pubsub.v1.PubsubMessage


// Logging
import org.slf4j.LoggerFactory

// Tracker
import com.snowplowanalytics.snowplow.scalatracker.Tracker

/**
 * Kafka Sink for Scala enrichment
 */
class PubsubSink(config: KinesisEnrichConfig,
    inputType: InputType.InputType, tracker: Option[Tracker]) extends ISink {

  private lazy val log = LoggerFactory.getLogger(getClass())
  import log.{error, debug, info, trace}

  private val topicName = inputType match {
    case InputType.Good => config.enrichedOutStream
    case InputType.Bad => config.badOutStream
  }

  private val pubsubPublisher = createPublisher(config)

  /**
   * Side-effecting function to store the EnrichedEvent
   * to the given output stream.
   *
   * EnrichedEvent takes the form of a tab-delimited
   * String until such time as https://github.com/snowplow/snowplow/issues/211
   * is implemented.
   *
   * This method blocks until the request has finished.
   *
   * @param events List of events together with their partition keys
   * @return whether to send the stored events to Kafka
   */
  def storeEnrichedEvents(events: List[(String, String)]): Boolean = {

    // Log BadRows
    inputType match {
      case InputType.Good => None
      case InputType.Bad  => events.foreach(e => debug(s"BadRow: ${e._1}"))
    }

    for ((value, _) <- events) {
      pubsubPublisher.publish(PubsubMessage.parseFrom(value.getBytes))
    }

    true // Always return true as our flush does nothing
  }

  /**
   * Blocking method to send all stored records to Kafka
   * For Pubsub this method doesn't do anything (we have
   * handed this off to the PubsubPublisher).
   */
  def flush() {
  }

  private def createPublisher(config: KinesisEnrichConfig): Publisher =
    Publisher.newBuilder(TopicName.parse(config.topicName)).build

}
