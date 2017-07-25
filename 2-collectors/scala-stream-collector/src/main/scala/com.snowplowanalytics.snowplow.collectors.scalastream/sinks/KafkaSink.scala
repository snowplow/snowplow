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

import java.util.Properties

import org.apache.kafka.clients.producer._

import model._

/**
 * Kafka Sink for the Scala collector
 */
class KafkaSink(config: CollectorConfig, inputType: InputType.InputType) extends Sink {

  // Records must not exceed MaxBytes - 1MB
  val MaxBytes = 1000000L

  private val topicName = inputType match {
    case InputType.Good => config.kafkaTopicGoodName
    case InputType.Bad  => config.kafkaTopicBadName
  }

  private var kafkaProducer = createProducer

  /**
   * Creates a new Kafka Producer with the given
   * configuration options
   *
   * @return a new Kafka Producer
   */
  private def createProducer: KafkaProducer[String, Array[Byte]] = {

    log.info(s"Create Kafka Producer to brokers: ${config.kafkaBrokers}")

    val props = new Properties()
    props.put("bootstrap.servers", config.kafkaBrokers)
    props.put("acks", "all")
    props.put("retries", "0")
    props.put("batch.size", config.byteLimit.toString)
    props.put("linger.ms", config.timeLimit.toString)
    props.put("key.serializer", 
      "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", 
      "org.apache.kafka.common.serialization.ByteArraySerializer")

    new KafkaProducer[String, Array[Byte]](props)
  }

  /**
   * Store raw events to the topic
   *
   * @param events The list of events to send
   * @param key The partition key to use
   */
  override def storeRawEvents(events: List[Array[Byte]], key: String) = {
    log.debug(s"Writing ${events.size} Thrift records to Kafka topic ${topicName} at key ${key}")
    events foreach {
      event => {
        try {
          kafkaProducer.send(new ProducerRecord(topicName, key, event))
        } catch {
          case e: Exception => {
            log.error(s"Unable to send event, see kafka log for more details: ${e.getMessage}")
            e.printStackTrace()
          }
        }
      }
    }
    Nil
  }

  override def getType = SinkType.Kafka
}
