package com.snowplowanalytics.snowplow.collectors.scalastream

import java.util.Properties

import com.typesafe.config.{Config, ConfigFactory}
import org.specs2.matcher.AnyMatchers
import org.specs2.mutable.Specification
import spray.testkit.Specs2RouteTest

class CollectorConfigSpec extends Specification with Specs2RouteTest with
  AnyMatchers {

  val testConf: Config = ConfigFactory.load("application.test.conf")
  val collectorConfig = new CollectorConfig(testConf)
  val props = collectorConfig.Kafka.Producer.getProps


  "Snowplow's Collector Configuration" should {
    "correctly parse Kafka configs" in {
      props.getProperty("bootstrap.server") must not be empty
      // default override
      props.getProperty("acks") must beEqualTo ("1")
      // timeout.ms is an additional property
      props.getProperty("timeout.ms") must beEqualTo ("60000")
      // default assertion
      props.getProperty("retries") must beEqualTo ("3")
    }

    "correctly convert a typesafe config to a java properties object" in {
      val config: Config = ConfigFactory.parseString(
        """
          |object {
          | string = "hello"
          | boolean = false
          | integer = 1
          |}
        """.stripMargin).getConfig("object")

      val properties = new Properties()
      properties.put("string", "hello")
      properties.put("boolean", "false")
      properties.put("integer", "1")

      CollectorConfig.propsFromConfig(config, Set()) must beEqualTo(properties)
    }

    "correctly blacklist" in {
      val config: Config = ConfigFactory.parseString(
        """
          |object {
          | string = "hello"
          | boolean = false
          | integer = 1
          |}
        """.stripMargin).getConfig("object")

      val properties = new Properties()
      properties.put("integer", "1")

      CollectorConfig.propsFromConfig(config, Set("string", "boolean")) must beEqualTo(properties)
    }
  }

}
