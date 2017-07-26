package com.snowplowanalytics.snowplow.enrich.kinesis

import java.util.Properties

import com.typesafe.config.{Config, ConfigFactory}
import org.specs2.matcher.AnyMatchers
import org.specs2.mutable.Specification


class EnrichConfigSpec extends Specification with
  AnyMatchers {

  val testConf: Config = ConfigFactory.load("application.test.conf")
  val collectorConfig = new EnrichConfig(testConf)
  val producerProps: Properties = collectorConfig.Kafka.Producer.getProps
  val consumerProps: Properties = collectorConfig.Kafka.Consumer.getProps

  "Snowplow's Collector Configuration" should {
    "correctly parse Kafka producer configs" in {
      producerProps.getProperty("bootstrap.servers") must not be null
      // default override
      producerProps.getProperty("acks") must beEqualTo ("1")
      // timeout.ms is an additional property
      producerProps.getProperty("timeout.ms") must beEqualTo ("60000")
      // default assertion
      producerProps.getProperty("retries") must beEqualTo ("3")
    }

    "correctly parse Kafka consumer configs" in {
      consumerProps.getProperty("bootstrap.servers") must not be null
      // default override
      consumerProps.getProperty("auto.offset.reset") must beEqualTo ("latest")
      // timeout.ms is an additional property
      consumerProps.getProperty("fetch.min.bytes") must beEqualTo ("2")
      // default assertion
      consumerProps.getProperty("auto.commit.interval.ms") must beEqualTo ("1000")
    }
  }

}
