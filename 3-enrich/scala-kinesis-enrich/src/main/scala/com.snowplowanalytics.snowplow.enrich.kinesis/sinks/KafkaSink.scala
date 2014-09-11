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
package com.snowplowanalytics.snowplow.enrich
package kinesis
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
import com.snowplowanalytics.snowplow.collectors.thrift._
import common.outputs.CanonicalOutput

/**
 * Kafka Sink for Scala enrichment
 */
class KafkaSink(config: KinesisEnrichConfig) extends ISink with Logging {
  private var kafka = createKafkaProducer(config)
  private val topic = config.kafkaEnrichedTopic

  def createKafkaProducer(config: KinesisEnrichConfig): Producer[AnyRef, AnyRef] = {
    val compress: Boolean = true
    val messageSendMaxRetries: Integer = 3
    val requestRequiredAcks: Integer = -1
    val clientId: String = InetAddress.getLocalHost.getHostName() + "_enrich"
    info(s"Create Kafka Producer '${clientId}' to brokers: ${config.kafkaBrokers}")

    val props = new Properties()
    val codec = if(compress) DefaultCompressionCodec.codec else NoCompressionCodec.codec
    props.put("compression.codec", codec.toString)
    props.put("producer.type", if(config.kafkaAsync) "async" else "sync")
    props.put("metadata.broker.list", config.kafkaBrokers)
    props.put("batch.num.messages", config.kafkaBatchSize.toString)
    props.put("message.send.max.retries", messageSendMaxRetries.toString)
    props.put("request.required.acks",requestRequiredAcks.toString)
    props.put("client.id",clientId.toString)
    val producer = new Producer[Object, Object](new ProducerConfig(props))
    info("producer created.")
    return producer
  }

  /**
   * Side-effecting function to store the CanonicalOutput
   * to the given output stream.
   *
   * CanonicalOutput takes the form of a tab-delimited
   * String until such time as https://github.com/snowplow/snowplow/issues/211
   * is implemented.
   */
  def storeCanonicalOutput(output: String, key: String) = {
    debug(s"Writing record to Kafka (topic '${topic}') : ${output}")
    try {
      kafka.send(new KeyedMessage(topic, output.getBytes("UTF8")))
    } catch {
      case e: Exception=>
        warn("unable to send event, see kafka log for more details")
    }
  }
}
