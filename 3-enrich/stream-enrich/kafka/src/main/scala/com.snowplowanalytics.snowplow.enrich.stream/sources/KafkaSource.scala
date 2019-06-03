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
package com.snowplowanalytics
package snowplow
package enrich
package stream
package sources

import java.util.Properties

import scala.collection.JavaConverters._
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer._
import scalaz._
import Scalaz._
import common.adapters.AdapterRegistry
import common.enrichments.EnrichmentRegistry
import iglu.client.Resolver
import model.{Kafka, StreamsConfig}
import scalatracker.Tracker
import sinks.{KafkaSink, Sink}

/** KafkaSubSource companion object with factory method */
object KafkaSource {
  def create(
    config: StreamsConfig,
    igluResolver: Resolver,
    adapterRegistry: AdapterRegistry,
    enrichmentRegistry: EnrichmentRegistry,
    tracker: Option[Tracker]
  ): Validation[String, KafkaSource] =
    for {
      kafkaConfig <- config.sourceSink match {
        case c: Kafka => c.success
        case _ => "Configured source/sink is not Kafka".failure
      }
      goodProducer <- KafkaSink
        .validateAndCreateProducer(kafkaConfig, config.buffer, config.out.enriched)
        .validation
      emitPii = utils.emitPii(enrichmentRegistry)
      _ <- utils.validatePii(emitPii, config.out.pii).validation
      piiProducer <- config.out.pii match {
        case Some(piiStreamName) =>
          KafkaSink
            .validateAndCreateProducer(kafkaConfig, config.buffer, piiStreamName)
            .validation
            .map(Some(_))
        case None => None.success
      }
      badProducer <- KafkaSink
        .validateAndCreateProducer(kafkaConfig, config.buffer, config.out.bad)
        .validation
    } yield new KafkaSource(
      goodProducer,
      piiProducer,
      badProducer,
      igluResolver,
      adapterRegistry,
      enrichmentRegistry,
      tracker,
      config,
      kafkaConfig
    )
}

/** Source to read events from a Kafka topic */
class KafkaSource private (
  goodProducer: KafkaProducer[String, String],
  piiProducer: Option[KafkaProducer[String, String]],
  badProducer: KafkaProducer[String, String],
  igluResolver: Resolver,
  adapterRegistry: AdapterRegistry,
  enrichmentRegistry: EnrichmentRegistry,
  tracker: Option[Tracker],
  config: StreamsConfig,
  kafkaConfig: Kafka
) extends Source(igluResolver, adapterRegistry, enrichmentRegistry, tracker, config.out.partitionKey) {

  override val MaxRecordSize = None

  override val threadLocalGoodSink: ThreadLocal[Sink] = new ThreadLocal[Sink] {
    override def initialValue: Sink =
      new KafkaSink(goodProducer, config.out.enriched)
  }

  override val threadLocalPiiSink: Option[ThreadLocal[Sink]] = piiProducer.flatMap {
    somePiiProducer =>
      config.out.pii.map { piiTopicName =>
        new ThreadLocal[Sink] {
          override def initialValue: Sink =
            new KafkaSink(somePiiProducer, piiTopicName)
        }
      }
  }

  override val threadLocalBadSink: ThreadLocal[Sink] = new ThreadLocal[Sink] {
    override def initialValue: Sink =
      new KafkaSink(badProducer, config.out.bad)
  }

  /** Never-ending processing loop over source stream. */
  override def run(): Unit = {
    val consumer = createConsumer(kafkaConfig.brokers, config.appName)

    log.info(s"Running Kafka consumer group: ${config.appName}.")
    log.info(s"Processing raw input Kafka topic: ${config.in.raw}")

    consumer.subscribe(List(config.in.raw).asJava)
    while (true) {
      val recordValues = consumer
        .poll(100) // Wait 100 ms if data is not available
        .asScala
        .toList
        .map(_.value) // Get the values

      enrichAndStoreEvents(recordValues)
    }
  }

  private def createConsumer(
    brokers: String,
    groupId: String
  ): KafkaConsumer[String, Array[Byte]] = {
    val properties = createProperties(brokers, groupId)
    properties.putAll(kafkaConfig.consumerConf.getOrElse(Map()).asJava)
    new KafkaConsumer[String, Array[Byte]](properties)
  }

  private def createProperties(brokers: String, groupId: String): Properties = {
    val props = new Properties()
    props.put("bootstrap.servers", brokers)
    props.put("group.id", groupId)
    props.put("enable.auto.commit", "true")
    props.put("auto.commit.interval.ms", "1000")
    props.put("auto.offset.reset", "earliest")
    props.put("session.timeout.ms", "30000")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer")
    props
  }
}
