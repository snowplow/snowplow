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
package stream 
package sinks

// Java
import java.util.Properties

// Kafka
import org.apache.kafka.clients.producer.{
  KafkaProducer,
  ProducerRecord
}

// Logging
import org.slf4j.LoggerFactory

// Tracker
import com.snowplowanalytics.snowplow.scalatracker.Tracker

/**
 * Kafka Sink for Scala enrichment
 */
class KafkaSink(config: KinesisEnrichConfig,
    inputType: InputType.InputType, tracker: Option[Tracker]) extends ISink {

  private lazy val log = LoggerFactory.getLogger(getClass())
  import log.{error, debug, info, trace}

  private val topicName = inputType match {
    case InputType.Good => config.enrichedOutStream
    case InputType.Bad => config.badOutStream
  }

  private val kafkaProducer = createProducer(config)

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

    for ((value, key) <- events) {
      val pr = new ProducerRecord(topicName, key, value)
      kafkaProducer.send(pr)
    }

    true // Always return true as our flush does nothing
  }

  /**
   * Blocking method to send all stored records to Kafka
   * For Kafka this method doesn't do anything (we have
   * handed this off to the KafkaProducer).
   */
  def flush() {
  }

  private def createProducer(config: KinesisEnrichConfig): KafkaProducer[String, String] = {
    val properties = createProperties(config)
    new KafkaProducer[String, String](properties)
  }

  private def createProperties(config: KinesisEnrichConfig): Properties = {

    val props = new Properties()
    props.put("bootstrap.servers", config.kafkaBrokers)
    props.put("acks", "all")
    props.put("retries", "0") // TODO yech
    props.put("batch.size", config.byteLimit.toString)
    props.put("linger.ms", config.timeLimit.toString)
    props.put("key.serializer",
      "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer",
      "org.apache.kafka.common.serialization.StringSerializer")
    props
  }

}
