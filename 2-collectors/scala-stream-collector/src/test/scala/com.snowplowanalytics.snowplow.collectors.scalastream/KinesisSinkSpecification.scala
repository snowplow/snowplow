package com.snowplowanalytics.snowplow
package collectors
package scalastream
package sinks

import org.scalacheck._
import org.scalacheck.Gen._
import org.scalacheck.Prop._
import org.scalacheck.Arbitrary._

object KinesisSinkGenerator {
  import com.snowplowanalytics.snowplow.collectors.scalastream.ConfigGenerator._

  lazy val genKinesisSink: Gen[KinesisSink] = for {
    config <- genCollectorConfig
  } yield new KinesisSink(config)

  implicit lazy val arbKinesisSink: Arbitrary[KinesisSink] = Arbitrary(genKinesisSink)
}

object KinesisSinkSpecification extends Properties("KinesisSink") {
  import KinesisSinkGenerator._

  /*property("check if stream exists") = forAll { kinesis: KinesisSink =>
    true
  }

  property("check raw event stored") = forAll { kinesis: KinesisSink =>
    true
  }*/
}