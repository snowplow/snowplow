/*
 * Copyright (c) 2013-2018 Snowplow Analytics Ltd. All rights reserved.
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
import scala.util.Try
import scala.collection.JavaConversions._
import scala.io.Source

// Scala libraries
import pureconfig._
import com.typesafe.config.ConfigFactory
import org.specs2.mutable.Specification

// Java
import java.util.regex.Pattern

// Java libraries
import org.apache.commons.codec.binary.Base64
import com.hubspot.jinjava.Jinjava

// This project
import good._
import model.EnrichConfig

class PiiEmitSpec extends Specification {

  import KafkaIntegrationSpecValues._

  val jinJava = new Jinjava()
  val configValues = Map(
    "sourceType" -> "kafka",
    "sinkType" -> "kafka",
    "streamsInRaw" -> s"$testGoodIn",
    "outEnriched" -> s"$testGood",
    "outPii" -> s"$testPii",
    "outBad" -> s"$testBad",
    "partitionKeyName" -> "\"\"",
    "kafkaBrokers" -> s"$kafkaHost",
    "region" -> "\"\"",
    "enrichStreamsOutMaxBackoff" -> "\"\"",
    "enrichStreamsOutMinBackoff" -> "\"\"",
    "nsqdPort" -> "123",
    "nsqlookupdPort" -> "234",
    "bufferTimeThreshold" -> "1",
    "bufferRecordThreshold" -> "1",
    "bufferByteThreshold" -> "100000",
    "enrichAppName" -> "Jim",
    "enrichStreamsOutMaxBackoff" -> "1000",
    "enrichStreamsOutMinBackoff" -> "1000",
    "appName" -> "jim")

  val configRes         = getClass.getResourceAsStream("/config.hocon.sample")
  val config            = Source.fromInputStream(configRes).getLines.mkString("\n")
  val configInstance    = jinJava.render(config, configValues)
  def decode(s: String) = Base64.decodeBase64(s)

  "Pii" should {
    "emit all events" in new KafkaIntegrationSpec {
      implicit def hint[T] = ProductHint[T](ConfigFieldMapping(CamelCase, CamelCase))
      val parsedConfig     = ConfigFactory.parseString(configInstance).resolve()
      val configObject     = Try { loadConfigOrThrow[EnrichConfig](parsedConfig.getConfig("enrich")) }
      configObject aka "enrichment config loading" must not beAFailedTry
      val app = getMainApplicationFuture(
        configObject.get,
        SpecHelpers.resolver,
        SpecHelpers.enrichmentRegistry,
        None)

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
      inputProduced aka "sending input" must beSuccessfulTry

      // Timeout for all the consumers (good, bad, and pii) (running in parallel)
      // You may want to adjust this if you are doing lots of slow work in the app
      // Ordinarily the consumers return in less than 1 sec
      override val consumerExecutionTimeoutSec = 15

      private def spaceJoinResult(expected: List[StringOrRegex]) =
        expected
          .flatMap({
            case JustRegex(r)                => Some(r.toString)
            case JustString(s) if s.nonEmpty => Some(Pattern.quote(s))
            case _                           => None
          })
          .mkString("\\s*")

      outputResult must beSuccessfulTry.like {
        case (good: List[String], bad: List[String], pii: List[String]) => {
          (bad aka "bad result list" must have size (expectedBad)) and
            (pii aka "pii result list" must have size (expectedPii)) and
            (good aka "good result list" must have size (expectedGood)) and
            (good aka "good result list" must containMatch(spaceJoinResult(PagePingWithContextSpec.expected))) and
            (pii aka "pii result list" must contain(PagePingWithContextSpec.pii)) and
            (good aka "good result list" must containMatch(spaceJoinResult(PageViewWithContextSpec.expected))) and
            (pii aka "pii result list" must contain(PageViewWithContextSpec.pii)) and
            (good aka "good result list" must containMatch(spaceJoinResult(StructEventSpec.expected))) and
            (pii aka "pii result list" must contain(StructEventSpec.pii)) and
            (good aka "good result list" must containMatch(spaceJoinResult(StructEventWithContextSpec.expected))) and
            (pii aka "pii result list" must contain(StructEventWithContextSpec.pii)) and
            (good aka "good result list" must containMatch(spaceJoinResult(TransactionItemSpec.expected))) and
            (pii aka "pii result list" must contain(TransactionItemSpec.pii)) and
            (good aka "good result list" must containMatch(spaceJoinResult(TransactionSpec.expected))) and
            (pii aka "pii result list" must contain(TransactionSpec.pii))
        }
      }
    }
  }
}
