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

import org.apache.kafka.common.serialization.{ByteArraySerializer, StringSerializer}

// Kafka
import org.apache.kafka.clients.producer._;

// Config
import com.typesafe.config.Config

// Logging
import org.slf4j.LoggerFactory

/**
 * Kafka Sink for the Scala collector
 */
class KafkaSink(config: CollectorConfig, inputType: InputType.InputType) extends AbstractSink {

  import log.{error, debug, info, trace}

  // Records must not exceed MaxBytes - 1MB
  val MaxBytes = 1000000L

  private val topicName = inputType match {
    case InputType.Good => config.Kafka.Topic.topicGoodName
    case InputType.Bad  => config.Kafka.Topic.topicBadName
  }

  private var kafkaProducer = createProducer

  /**
   * Creates a new Kafka Producer with the given
   * configuration options
   *
   * @return a new Kafka Producer
   */
  private def createProducer: KafkaProducer[String, Array[Byte]] = {

    val kafkaProps = config.Kafka.Producer.getProps
    info(s"Create Kafka Producer to brokers: ${kafkaProps.getProperty("bootstrap.servers")}")
    new KafkaProducer[String, Array[Byte]](kafkaProps)
  }

  /**
   * Store raw events to the topic
   *
   * @param events The list of events to send
   * @param key The partition key to use
   */
  override def storeRawEvents(events: List[Array[Byte]], key: String) = {
    debug(s"Writing ${events.size} Thrift records to Kafka topic ${topicName} at key ${key}")
    events foreach {
      event => {
        kafkaProducer.send(new ProducerRecord(topicName, key, event), new Callback() {
          override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
            if(exception != null)
              error(s"Unable to send event, see kafka log for more details: ${exception.getMessage}")
              exception.printStackTrace()
          }
        })
      }
    }
    Nil
  }

  override def getType = Sink.Kafka
}
