/*
 * Copyright (c) 2013-2019 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.collectors.scalastream
package sinks

import java.util.Properties

import scala.collection.JavaConverters._

import org.apache.kafka.clients.producer._

import model._

/**
 * Kafka Sink for the Scala collector
 */
class KafkaSink(
  kafkaConfig: Kafka,
  bufferConfig: BufferConfig,
  topicName: String
) extends Sink {

  // Records must not exceed MaxBytes - 1MB
  val MaxBytes = 1000000

  private val kafkaProducer = createProducer

  /**
   * Creates a new Kafka Producer with the given
   * configuration options
   *
   * @return a new Kafka Producer
   */
  private def createProducer: KafkaProducer[String, Array[Byte]] = {

    log.info(s"Create Kafka Producer to brokers: ${kafkaConfig.brokers}")

    val props = new Properties()
    props.put("bootstrap.servers", kafkaConfig.brokers)
    props.put("acks", "all")
    props.put("retries", kafkaConfig.retries.toString)
    props.put("buffer.memory", bufferConfig.byteLimit.toString)
    props.put("linger.ms", bufferConfig.timeLimit.toString)
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")

    props.putAll(kafkaConfig.producerConf.getOrElse(Map()).asJava)

    new KafkaProducer[String, Array[Byte]](props)
  }

  /**
   * Store raw events to the topic
   *
   * @param events The list of events to send
   * @param key The partition key to use
   */
  override def storeRawEvents(events: List[Array[Byte]], key: String): List[Array[Byte]] = {
    log.debug(s"Writing ${events.size} Thrift records to Kafka topic $topicName at key $key")
    events.foreach { event =>
      kafkaProducer.send(
        new ProducerRecord(topicName, key, event),
        new Callback {
          override def onCompletion(metadata: RecordMetadata, e: Exception): Unit =
            if (e != null) log.error(s"Sending event failed: ${e.getMessage}")
        }
      )
    }
    Nil
  }
}
