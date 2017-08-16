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
package com.snowplowanalytics
package snowplow
package enrich
package stream
package sources

import java.net.InetAddress
import java.util.{Properties, UUID}

import scala.collection.JavaConverters._

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.slf4j.LoggerFactory

import iglu.client.Resolver
import common.enrichments.EnrichmentRegistry
import scalatracker.Tracker

/**
 * Source to read events from a Kafka topic
 */
class KafkaSource(
  config: KinesisEnrichConfig,
  igluResolver: Resolver,
  enrichmentRegistry: EnrichmentRegistry,
  tracker: Option[Tracker]
) extends AbstractSource(config, igluResolver, enrichmentRegistry, tracker) {

  lazy val log = LoggerFactory.getLogger(getClass())

  /**
   * Never-ending processing loop over source stream.
   */
  override def run(): Unit = {
    val workerId = InetAddress.getLocalHost().getCanonicalHostName() +
      ":" + UUID.randomUUID()
    log.info("Using workerId: " + workerId)

    val consumer = createConsumer(config)

    log.info(s"Running Kafka consumer group: ${config.appName}.")
    log.info(s"Processing raw input Kafka topic: ${config.rawInStream}")

    consumer.subscribe(List(config.rawInStream).asJava)
    while (true) {
      val recordValues = consumer
        .poll(100)    // Wait 100 ms if data is not available
        .asScala
        .toList
        .map(_.value) // Get the values

      enrichAndStoreEvents(recordValues)
    }
  }

  private def createConsumer(config: KinesisEnrichConfig): KafkaConsumer[String, Array[Byte]] = {
    val properties = createProperties(config)
    new KafkaConsumer[String, Array[Byte]](properties)
  }

  private def createProperties(config: KinesisEnrichConfig): Properties = {

    val props = new Properties()
    props.put("bootstrap.servers", config.kafkaBrokers)
    props.put("group.id", config.appName)
    props.put("enable.auto.commit", "true")
    props.put("auto.commit.interval.ms", "1000")
    props.put("auto.offset.reset", "earliest")
    props.put("session.timeout.ms", "30000")
    props.put("key.deserializer",
      "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer",
      "org.apache.kafka.common.serialization.ByteArrayDeserializer")
    props
  }
}
