/*
 * Copyright (c) 2013-2014 Snowplow Analytics Ltd. All rights reserved.
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
import java.net.InetAddress

// Kafka
import kafka.common._
import kafka.utils._
import kafka.message._
import kafka.producer._

// Config
import com.typesafe.config.Config

// Snowplow
import scalastream._
import thrift.SnowplowRawEvent



/**
 * Kafka Sink for the Scala collector.
 */
class KafkaSink(config: CollectorConfig) extends AbstractSink with Logging {
  private var kafka = createKafkaProducer(config: CollectorConfig)
  private val topic = config.kafkaTopic

  def createKafkaProducer(config: CollectorConfig): Producer[AnyRef, AnyRef] = {
    val compress: Boolean = true
    val messageSendMaxRetries: Integer = 3
    val requestRequiredAcks: Integer = -1
    val clientId: String = InetAddress.getLocalHost.getHostName()
    info(s"Create Kafka Producer '${clientId}' to brokers: ${config.brokers}")

    val props = new Properties()
    val codec = if(compress) DefaultCompressionCodec.codec else NoCompressionCodec.codec
    props.put("compression.codec", codec.toString)
    props.put("producer.type", if(config.kafkaAsync) "async" else "sync")
    props.put("metadata.broker.list", config.brokers)
    props.put("batch.num.messages", config.kafkaBatchSize.toString)
    props.put("message.send.max.retries", messageSendMaxRetries.toString)
    props.put("request.required.acks",requestRequiredAcks.toString)
    props.put("client.id",clientId.toString)
    val producer = new Producer[Object, Object](new ProducerConfig(props))
    info("producer created.")
    return producer
  }

  def storeRawEvent(event: SnowplowRawEvent, key: String): Array[Byte] = {
    debug(s"Writing Thrift record to Kafka: ${event.toString}")
    val se = serializeEvent(event)
    try {
      kafka.send(new KeyedMessage(topic, se))
    } catch {
      case e: Exception=>
        warn("unable to send event, see kafka log for more details")
    }
    return se
  }
}

// vim: set ts=2 sw=2:
