/*
 * Copyright (c) 2013-2019 Snowplow Analytics Ltd.
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
package enrich.stream
package sinks

import java.util.Properties

import scala.collection.JavaConverters._

import org.apache.kafka.clients.producer._

import scalaz._
import Scalaz._

import model.{BufferConfig, Kafka}

/** KafkaSink companion object with factory method */
object KafkaSink {
  def validateAndCreateProducer(
    kafkaConfig: Kafka,
    bufferConfig: BufferConfig,
    topicName: String
  ): \/[String, KafkaProducer[String, String]] =
    createProducer(kafkaConfig, bufferConfig).right

  /**
   * Instantiates a producer on an existing topic with the given configuration options.
   * This can fail if the producer can't be created.
   * @return a Kafka producer
   */
  private def createProducer(
    kafkaConfig: Kafka,
    bufferConfig: BufferConfig
  ): KafkaProducer[String, String] = {
    val properties = createProperties(kafkaConfig, bufferConfig)
    properties.putAll(kafkaConfig.producerConf.getOrElse(Map()).asJava)
    new KafkaProducer[String, String](properties)
  }

  private def createProperties(kafkaConfig: Kafka, bufferConfig: BufferConfig): Properties = {
    val props = new Properties()
    props.put("bootstrap.servers", kafkaConfig.brokers)
    props.put("acks", "all")
    props.put("retries", kafkaConfig.retries.toString)
    props.put("buffer.memory", bufferConfig.byteLimit.toString)
    props.put("linger.ms", bufferConfig.timeLimit.toString)
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props
  }
}

/** Kafka Sink for Scala enrichment */
class KafkaSink(kafkaProducer: KafkaProducer[String, String], topicName: String) extends Sink {

  /**
   * Side-effecting function to store the EnrichedEvent to the given output stream.
   * EnrichedEvent takes the form of a tab-delimited String until such time as
   * https://github.com/snowplow/snowplow/issues/211 is implemented.
   * @param events List of events together with their partition keys
   * @return whether to send the stored events to Kafka
   */
  override def storeEnrichedEvents(events: List[(String, String)]): Boolean = {
    for ((value, key) <- events) {
      kafkaProducer.send(
        new ProducerRecord(topicName, key, value),
        new Callback {
          override def onCompletion(metadata: RecordMetadata, e: Exception): Unit =
            if (e != null) log.error(s"Sending event failed: ${e.getMessage}")
        }
      )
    }
    true
  }

  /** Blocking method to send all buffered records to Kafka. */
  override def flush(): Unit = kafkaProducer.flush()

}
