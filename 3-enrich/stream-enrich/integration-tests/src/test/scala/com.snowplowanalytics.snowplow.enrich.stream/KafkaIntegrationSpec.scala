/*
 * Copyright (c) 2013-2019 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0, and
 * you may not use this file except in compliance with the Apache License
 * Version 2.0.  You may obtain a copy of the Apache License Version 2.0 at
 * http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the Apache License Version 2.0 is distributed on an "AS
 * IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the Apache License Version 2.0 for the specific language
 * governing permissions and limitations there under.
 */

package com.snowplowanalytics
package snowplow
package enrich
package stream

// Scala
import common.adapters.AdapterRegistry
import enrich.stream.model.StreamsConfig

import scala.concurrent._
import scala.concurrent.duration.Duration
import scala.util.Try
import collection.JavaConversions._
import scala.concurrent.forkjoin.ForkJoinPool

// Java
import java.util.Properties

// Scala libraries
import org.apache.kafka.clients.consumer.{ConsumerRecords, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

// Specs2
import org.specs2.matcher.{TraversableMatchers, TryMatchers}

// Snowplow and Iglu
import scalatracker.Tracker
import iglu.client.Resolver
import common.enrichments.EnrichmentRegistry

/*
 * Extending this trait creates a new integration test with a new instance of kafka
 * See PiiEmitSpec for an example of how to use it
 */
trait KafkaIntegrationSpec
    extends TryMatchers
    with TraversableMatchers {

  import KafkaIntegrationSpecValues._
  implicit val ec = ExecutionContext.fromExecutor(new ForkJoinPool(16))
  val kafkaTopics = Set(testGoodIn, testGood, testBad, testPii)

  def expectedGood: Int
  def expectedBad: Int
  def expectedPii: Int

  def inputGood: List[Array[Byte]]

  def getMainApplicationFuture(
                                configuration: StreamsConfig,
                                resolver: Resolver,
                                adapterRegistry: AdapterRegistry,
                                enrichmentRegistry: EnrichmentRegistry,
                                tracker: Option[Tracker]): Future[Unit] = Future {
    KafkaEnrich.getSource(configuration, resolver, adapterRegistry, enrichmentRegistry, tracker).toOption.get.run()
  }

  def producerTimeoutSec: Int
  def inputProduced(address: String): Try[Unit] =
    Try { Await.result(produce(address: String), Duration(s"$producerTimeoutSec sec")) }
  def testKafkaPropertiesProducer(address: String) = {
      val props = new Properties()
      props.put("bootstrap.servers", address)
      props.put("client.id", "producer-george")
      props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")
      props
    }
  def produce(address: String): Future[Unit] = Future {
    val testProducer = new KafkaProducer[String, Array[Byte]](testKafkaPropertiesProducer(address))
    val events = inputGood
    events.foreach { r =>
      testProducer.send(new ProducerRecord(testGoodIn, "key", r))
    }
    testProducer.flush
    testProducer.close
  }
  private def getListOfRecords(cr: ConsumerRecords[String, String]): List[String] =
    cr.map(_.value).toList

  val POLL_TIME_MSEC = 100L

  def getRecords(topic: String, expectedRecords: Int, timeoutSec: Int, address: String): Future[List[String]] =
    Future {
      val started = System.currentTimeMillis
      val testKafkaPropertiesConsumer = {
        val props = new Properties()
        props.put("bootstrap.servers", address)
        props.put("auto.offset.reset", "earliest")
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
        props
          .put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
        props.put("group.id", s"consumer-$topic")
        props
      }
      val testConsumerPii = new KafkaConsumer[String, String](testKafkaPropertiesConsumer)
      testConsumerPii.subscribe(List(topic))
      var records = getListOfRecords(testConsumerPii.poll(POLL_TIME_MSEC))
      while (((System.currentTimeMillis - started) / 1000 < timeoutSec - 1) && records.size < expectedRecords) {
        records = records ++ getListOfRecords(testConsumerPii.poll(POLL_TIME_MSEC))
      }
      testConsumerPii.close()
      records
    }

  def consumerExecutionTimeoutSec: Int
  def producedBadRecords(address: String): Future[List[String]] =
    getRecords(testBad, expectedBad, consumerExecutionTimeoutSec, address)
  def producedGoodRecords(address: String): Future[List[String]] =
    getRecords(testGood, expectedGood, consumerExecutionTimeoutSec, address)
  def producedPiiRecords(address: String): Future[List[String]] =
    getRecords(testPii, expectedPii, consumerExecutionTimeoutSec, address)
  def allResults(address: String): Future[(List[String], List[String], List[String])] =
    for {
      good <- producedGoodRecords(address)
      bad  <- producedBadRecords(address)
      pii  <- producedPiiRecords(address)
    } yield (good, bad, pii)

}

object KafkaIntegrationSpecValues {
  val (testGoodIn, testGood, testBad, testPii) =
    ("testGoodIn", "testEnrichedGood", "testEnrichedBad", "testEnrichedUglyPii")
}

