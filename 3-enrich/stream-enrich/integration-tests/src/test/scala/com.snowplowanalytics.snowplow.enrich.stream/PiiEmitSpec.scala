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
package com.snowplowanalytics.snowplow.enrich.stream

import java.util.regex.Pattern
import java.util.concurrent.TimeUnit

import scala.util.{Failure, Success, Try}
import scala.collection.JavaConverters._
import scala.concurrent.duration.FiniteDuration
import scala.io.Source

import com.hubspot.jinjava.Jinjava
import com.typesafe.config.ConfigFactory
import org.apache.commons.codec.binary.Base64
import org.specs2.concurrent.ExecutionEnv
import org.specs2.mutable.Specification
import org.specs2.matcher.{FutureMatchers, Matcher}
import org.specs2.specification.BeforeAfterAll
import pureconfig._
import pureconfig.generic.auto._
import pureconfig.generic.{FieldCoproductHint, ProductHint}

import good._
import model.{SourceSinkConfig, StreamsConfig}

class PiiEmitSpec(implicit ee: ExecutionEnv)
    extends Specification
    with FutureMatchers
    with KafkaIntegrationSpec
    with BeforeAfterAll {

  var ktu: KafkaTestUtils = _
  override def beforeAll(): Unit = {
    ktu = new KafkaTestUtils
    ktu.setup()
    ktu.createTopics(kafkaTopics.toList: _*)
  }
  override def afterAll(): Unit =
    if (ktu != null) {
      ktu = null
    }

  import KafkaIntegrationSpecValues._

  def configValues = Map(
    "sinkType" -> "kafka",
    "streamsInRaw" -> s"$testGoodIn",
    "outEnriched" -> s"$testGood",
    "outPii" -> s"$testPii",
    "outBad" -> s"$testBad",
    "partitionKeyName" -> "\"\"",
    "kafkaBrokers" -> ktu.brokerAddress,
    "bufferTimeThreshold" -> "1",
    "bufferRecordThreshold" -> "1",
    "bufferByteThreshold" -> "100000",
    "enrichAppName" -> "Jim",
    "enrichStreamsOutMaxBackoff" -> "1000",
    "enrichStreamsOutMinBackoff" -> "1000",
    "appName" -> "jim"
  )

  def config: String =
    Try {
      val configRes = getClass.getResourceAsStream("/config.hocon.sample")
      Source.fromInputStream(configRes).getLines.mkString("\n")
    } match {
      case Failure(t) => {
        println(s"Unable to get config.hocon.sample: $t"); throw new Exception(t)
      }
      case Success(s) => s
    }

  def configInstance: String = {
    val jinJava = new Jinjava()
    jinJava.render(config, configValues.asJava)
  }

  private def decode(s: String): Array[Byte] = Base64.decodeBase64(s)

  // Input
  override val inputGood = List(
    decode(PagePingWithContextSpec.raw),
    decode(PageViewWithContextSpec.raw),
    decode(StructEventSpec.raw),
    decode(StructEventWithContextSpec.raw),
    decode(TransactionItemSpec.raw),
    decode(TransactionSpec.raw)
  )
  // Expected output counts
  override val (expectedGood, expectedBad, expectedPii) = (inputGood.size, 0, inputGood.size)

  // Timeout for the producer
  override val producerTimeoutSec = 5

  // Timeout for all the consumers (good, bad, and pii) (running in parallel)
  // You may want to adjust this if you are doing lots of slow work in the app
  // Ordinarily the consumers return in less than 1 sec
  override val consumerExecutionTimeoutSec = 15

  implicit def hint[T]: ProductHint[T] =
    ProductHint[T](ConfigFieldMapping(CamelCase, CamelCase))
  implicit val _: FieldCoproductHint[SourceSinkConfig] =
    new FieldCoproductHint[SourceSinkConfig]("enabled")

  "Pii" should {
    "emit all events" in {

      val parsedConfig = ConfigFactory.parseString(configInstance).resolve()
      val configObject = Try {
        loadConfigOrThrow[StreamsConfig](parsedConfig.getConfig("enrich.streams"))
      }
      configObject aka "enrichment config loading" must not beAFailedTry

      getMainApplicationFuture(
        configObject.get,
        SpecHelpers.client,
        SpecHelpers.adapterRegistry,
        SpecHelpers.enrichmentRegistry,
        None
      )
      inputProduced(ktu.brokerAddress) aka "sending input" must beSuccessfulTry

      def spaceJoinResult(expected: List[StringOrRegex]) =
        expected
          .flatMap({
            case JustRegex(r) => Some(r.toString)
            case JustString(s) if s.nonEmpty => Some(Pattern.quote(s))
            case _ => None
          })
          .mkString("\\s*")

      val expectedMatcher: Matcher[(List[String], List[String], List[String])] = beLike {
        case (good: List[String], bad: List[String], pii: List[String]) => {
          bad aka "bad result list" must have size (expectedBad)
          pii aka "pii result list" must have size (expectedPii)
          good aka "good result list" must have size (expectedGood)
          good aka "good result list" must containMatch(
            spaceJoinResult(PagePingWithContextSpec.expected)
          )
          pii aka "pii result list" must containMatch(spaceJoinResult(PagePingWithContextSpec.pii))
          good aka "good result list" must containMatch(
            spaceJoinResult(PageViewWithContextSpec.expected)
          )
          pii aka "pii result list" must containMatch(spaceJoinResult(PageViewWithContextSpec.pii))
          good aka "good result list" must containMatch(spaceJoinResult(StructEventSpec.expected))
          pii aka "pii result list" must containMatch(spaceJoinResult(StructEventSpec.pii))
          good aka "good result list" must containMatch(
            spaceJoinResult(StructEventWithContextSpec.expected)
          )
          pii aka "pii result list" must containMatch(
            spaceJoinResult(StructEventWithContextSpec.pii)
          )
          good aka "good result list" must containMatch(
            spaceJoinResult(TransactionItemSpec.expected)
          )
          pii aka "pii result list" must containMatch(spaceJoinResult(TransactionItemSpec.pii))
          good aka "good result list" must containMatch(spaceJoinResult(TransactionSpec.expected))
          pii aka "pii result list" must containMatch(spaceJoinResult(TransactionSpec.pii))
        }
      }
      allResults(ktu.brokerAddress) must expectedMatcher.await(
        retries = 0,
        timeout = FiniteDuration(consumerExecutionTimeoutSec.toLong, TimeUnit.SECONDS)
      )
    }
  }
}
