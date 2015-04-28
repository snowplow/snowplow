package com.snowplowanalytics.snowplow
package collectors
package scalastream

import org.scalacheck._
import org.scalacheck.Arbitrary._
import org.scalacheck.Gen._
import org.scalacheck.Prop._
import com.typesafe.config.{ ConfigFactory, Config }

object ConfigGenerator {
  val testConf: Config =
    ConfigFactory.parseString("""
collector {
  interface = "0.0.0.0"
  port = 8080

  production = true

  p3p {
    policyref = "/w3c/p3p.xml"
    CP = "NOI DSP COR NID PSA OUR IND COM NAV STA"
  }

  cookie {
    expiration = 365 days
    domain = "test-domain.com"
  }

  sink {
    enabled = "test"

    kinesis {
      aws {
        access-key: "cpf"
        secret-key: "cpf"
      }
      stream {
        region: "us-east-1"
        name: "snowplow_collector_example"
        size: 1
      }
    }
  }
}
""".stripMargin)

  lazy val genConfig: Gen[Config] = frequency((4, testConf), (1, ConfigFactory.load("sample")))

  lazy val genCollectorConfig: Gen[CollectorConfig] = for {
    gen <- genConfig
  } yield new CollectorConfig {
    override val config = gen
  }

  implicit lazy val arbConfig: Arbitrary[Config] = Arbitrary(genConfig)

}

object CollectorConfigSpecification extends Properties("Config") {
  import ConfigGenerator._

  property("Testing public api of Collector Config trait") = forAll { conf: Config =>
    val isTestConf = conf.getConfig("collector").getConfig("sink").getString("enabled")

    val collectorConfig = new CollectorConfig {
      override val config = conf
    }

    isTestConf match {
      case "test"    => withTestConf(collectorConfig)
      case "kinesis" => withKinesisConf(collectorConfig)
    }
  }

  private def withKinesisConf(collectorConfig: CollectorConfig): Boolean = {

    val sampleConf = new CollectorConfig {
      override val config = ConfigFactory.load("sample")
    }

    val sinkEnabled = collectorConfig.sinkEnabled == sampleConf.sinkEnabled
    val awsAccessKey = collectorConfig.awsAccessKey == sampleConf.awsAccessKey
    val awsSecretKey = collectorConfig.awsSecretKey == sampleConf.awsSecretKey

    sinkEnabled && awsAccessKey && awsSecretKey
  }

  private def withTestConf(collectorConfig: CollectorConfig): Boolean = {
    val sinkEnabled = collectorConfig.sinkEnabled == "test"
    val p3pPolicyRef = collectorConfig.p3pPolicyRef == "/w3c/p3p.xml"
    val p3pCP = collectorConfig.p3pCP == "NOI DSP COR NID PSA OUR IND COM NAV STA"

    import scala.concurrent.duration._
    val cookieExpiration = collectorConfig.cookieExpiration.equals(31536000000L) //365 days in Long
    val cookieDomain = collectorConfig.cookieDomain.get == "test-domain.com"
    val awsAccessKey = collectorConfig.awsAccessKey == "cpf"
    val awsSecretKey = collectorConfig.awsSecretKey == "cpf"
    val streamName = collectorConfig.streamName == "snowplow_collector_example"
    val streamSize = collectorConfig.streamSize == 1
    val streamEndpoint = collectorConfig.streamEndpoint == "https://kinesis.us-east-1.amazonaws.com"

    sinkEnabled && p3pPolicyRef && p3pCP &&
      cookieExpiration && cookieDomain && awsAccessKey &&
      awsSecretKey && streamName && streamSize && streamEndpoint
  }
}