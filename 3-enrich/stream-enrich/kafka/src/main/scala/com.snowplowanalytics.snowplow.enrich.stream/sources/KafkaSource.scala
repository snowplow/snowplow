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
package com.snowplowanalytics.snowplow.enrich.stream
package sources

import java.util.Properties

import scala.collection.JavaConverters._

import cats.Id
import cats.syntax.either._
import com.snowplowanalytics.iglu.client.Client
import com.snowplowanalytics.snowplow.badrows.Processor
import com.snowplowanalytics.snowplow.enrich.common.adapters.AdapterRegistry
import com.snowplowanalytics.snowplow.enrich.common.enrichments.EnrichmentRegistry
import io.circe.Json
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer._

import model.{Kafka, StreamsConfig}
import sinks.{KafkaSink, Sink}
import java.time.Duration

/** KafkaSubSource companion object with factory method */
object KafkaSource {
  def create(
    config: StreamsConfig,
    client: Client[Id, Json],
    adapterRegistry: AdapterRegistry,
    enrichmentRegistry: EnrichmentRegistry[Id],
    processor: Processor
  ): Either[String, KafkaSource] =
    for {
      kafkaConfig <- config.sourceSink match {
        case c: Kafka => c.asRight
        case _ => "Configured source/sink is not Kafka".asLeft
      }
      goodProducer <- KafkaSink.validateAndCreateProducer(kafkaConfig, config.buffer)
      emitPii = utils.emitPii(enrichmentRegistry)
      _ <- utils.validatePii(emitPii, config.out.pii)
      piiProducer <- config.out.pii match {
        case Some(_) =>
          KafkaSink
            .validateAndCreateProducer(kafkaConfig, config.buffer)
            .map(Some(_))
        case None => None.asRight
      }
      badProducer <- KafkaSink.validateAndCreateProducer(kafkaConfig, config.buffer)
    } yield new KafkaSource(
      goodProducer,
      piiProducer,
      badProducer,
      client,
      adapterRegistry,
      enrichmentRegistry,
      processor,
      config,
      kafkaConfig
    )
}

/** Source to read events from a Kafka topic */
class KafkaSource private (
  goodProducer: KafkaProducer[String, String],
  piiProducer: Option[KafkaProducer[String, String]],
  badProducer: KafkaProducer[String, String],
  client: Client[Id, Json],
  adapterRegistry: AdapterRegistry,
  enrichmentRegistry: EnrichmentRegistry[Id],
  processor: Processor,
  config: StreamsConfig,
  kafkaConfig: Kafka
) extends Source(client, adapterRegistry, enrichmentRegistry, processor, config.out.partitionKey) {

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
        .poll(Duration.ofMillis(100)) // Wait 100 ms if data is not available
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
