package com.snowplowanalytics
package snowplow
package enrich
package stream

// Scala
import scala.concurrent._
import scala.concurrent.duration.Duration
import scala.util.Try
import collection.JavaConversions._

// Java
import java.util.Properties

// Scala libraries
import org.apache.kafka.clients.consumer.{ConsumerRecords, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import io.bfil.kafka.specs2.DefaultKafkaPorts
import io.bfil.kafka.specs2.EmbeddedKafkaContext

// Specs2
import org.specs2.matcher.{TraversableMatchers, TryMatchers}

// Snowplow and Iglu
import model.EnrichConfig
import scalatracker.Tracker
import iglu.client.Resolver
import common.enrichments.EnrichmentRegistry

/*
 * Extending this trait creates a new integration test with a new instance of kafka
 * See PiiEmitSpec for an example of how to use it
 */

trait KafkaIntegrationSpec
    extends EmbeddedKafkaContext
    with DefaultKafkaPorts
    with TryMatchers
    with TraversableMatchers {

  import KafkaIntegrationSpecValues._
  import ExecutionContext.Implicits.global

  val kafkaTopics = Set(testGoodIn, testGood, testBad, testPii)

  def expectedGood: Int
  def expectedBad: Int
  def expectedPii: Int

  def inputGood: List[Array[Byte]]

  def getMainApplicationFuture(
    configuration: EnrichConfig,
    resolver: Resolver,
    registry: EnrichmentRegistry,
    tracker: Option[Tracker]) = Future {
    EnrichApp.run(configuration, resolver, registry, tracker)
  }

  def producerTimeoutSec: Int
  def inputProduced = Try { Await.result(producer, Duration(s"$producerTimeoutSec sec")) }

  def producer = Future {
    val testKafkaPropertiesProducer = {
      val props = new Properties()
      props.put("bootstrap.servers", kafkaHost)
      props.put("client.id", "producer-george")
      props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")
      props
    }
    val testProducer = new KafkaProducer[String, Array[Byte]](testKafkaPropertiesProducer)

    val events = inputGood
    events.foreach { r =>
      testProducer.send(new ProducerRecord(testGoodIn, "key", r))
    }
    testProducer.flush
    testProducer.close
  }
  private def getRecords(cr: ConsumerRecords[String, String]): List[String] =
    cr.map(_.value).toList

  def getConsumer(topic: String, expectedRecords: Int, timeoutSec: Int) = Future {
    val started = System.currentTimeMillis
    val testKafkaPropertiesConsumer = {
      val props = new Properties()
      props.put("bootstrap.servers", kafkaHost)
      props.put("auto.offset.reset", "earliest")
      props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
      props
        .put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
      props.put("group.id", s"consumer-$topic")
      props
    }
    val testConsumerPii = new KafkaConsumer[String, String](testKafkaPropertiesConsumer)
    testConsumerPii.subscribe(List(topic))
    var records = getRecords(testConsumerPii.poll(100))
    while (((System.currentTimeMillis - started) / 1000 < timeoutSec - 1) && records.size < expectedRecords) {
      records = records ++ getRecords(testConsumerPii.poll(100))
    }
    records
  }

  def consumerExecutionTimeoutSec: Int
  def producedBadRecords  = getConsumer(testBad, expectedBad, consumerExecutionTimeoutSec)
  def producedGoodRecords = getConsumer(testGood, expectedGood, consumerExecutionTimeoutSec)
  def producedPiiRecords  = getConsumer(testPii, expectedPii, consumerExecutionTimeoutSec)
  def allFutures =
    for {
      good <- producedGoodRecords
      bad  <- producedBadRecords
      pii  <- producedPiiRecords
    } yield (good, bad, pii)
  def outputResult = Try {
    Await.result(allFutures, Duration(s"$consumerExecutionTimeoutSec sec"))
  }

}

object KafkaIntegrationSpecValues {
  val (testGoodIn, testGood, testBad, testPii) =
    ("testGoodIn", "testEnrichedGood", "testEnrichedBad", "testEnrichedUglyPii")
  val kafkaHost = "127.0.0.1:9092"
}
