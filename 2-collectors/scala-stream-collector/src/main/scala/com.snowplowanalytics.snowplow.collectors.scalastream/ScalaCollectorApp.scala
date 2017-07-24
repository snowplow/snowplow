/* 
 * Copyright (c) 2013-2016 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow
package collectors
package scalastream

import java.io.File
import java.util.concurrent.ScheduledThreadPoolExecutor
import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.stream.ActorMaterializer
import com.typesafe.config.{ConfigFactory, Config}
import org.slf4j.LoggerFactory

import model._
import sinks._

// Main entry point of the Scala collector.
object ScalaCollector extends App {
  lazy val log = LoggerFactory.getLogger(getClass())

  case class FileConfig(config: File = new File("."))
  val parser = new scopt.OptionParser[FileConfig](generated.Settings.name) {
    head(generated.Settings.name, generated.Settings.version)
    opt[File]("config").required().valueName("<filename>")
      .action((f: File, c: FileConfig) => c.copy(f))
      .validate(f =>
        if (f.exists) success
        else failure(s"Configuration file $f does not exist")
      )
  }

  val conf = parser.parse(args, FileConfig()) match {
    case Some(c) => ConfigFactory.parseFile(c.config).resolve()
    case None    => ConfigFactory.empty()
  }

  if (conf.isEmpty()) {
    System.err.println("Empty configuration file")
    System.exit(1)
  }

  val collectorConfig = new CollectorConfig(conf)

  implicit val system = ActorSystem.create("scala-stream-collector", conf)
  implicit val materializer = ActorMaterializer()
  implicit val executionContext = system.dispatcher

  lazy val executorService = new ScheduledThreadPoolExecutor(collectorConfig.threadpoolSize)

  val sinks = collectorConfig.sinkEnabled match {
    case SinkType.Kinesis => {
      val good = KinesisSink.createAndInitialize(collectorConfig, InputType.Good, executorService)
      val bad  = KinesisSink.createAndInitialize(collectorConfig, InputType.Bad, executorService)
      CollectorSinks(good, bad) 
    }
    case SinkType.Kafka => {
      val good = new KafkaSink(collectorConfig, InputType.Good)
      val bad  = new KafkaSink(collectorConfig, InputType.Bad)
      CollectorSinks(good, bad)
    }
    case SinkType.Stdout  => {
      val good = new StdoutSink(InputType.Good)
      val bad = new StdoutSink(InputType.Bad)
      CollectorSinks(good, bad) 
    }
  }

  val route = new CollectorRoute {
    override def collectorService = new CollectorService(collectorConfig, sinks)
  }

  Http().bindAndHandle(route.collectorRoute, collectorConfig.interface, collectorConfig.port).map { binding =>
    log.info(s"REST interface bound to ${binding.localAddress}")
  } recover { case ex =>
    log.error("REST interface could not be bound to " +
      s"${collectorConfig.interface}:${collectorConfig.port}", ex.getMessage)
  }
}

// Return Options from the configuration.
object Helper {
  implicit class RichConfig(val underlying: Config) extends AnyVal {
    def getOptionalString(path: String): Option[String] =
      if (underlying.hasPath(path)) Some(underlying.getString(path))
      else None
  }
}

// How a collector should set cookies
case class CookieConfig(name: String, expiration: Long, domain: Option[String])

// Rigidly load the configuration file here to error when
// the collector process starts rather than later.
class CollectorConfig(config: Config) {
  import Helper.RichConfig

  private val collector = config.getConfig("collector")
  val interface = collector.getString("interface")
  val port = collector.getInt("port")
  val production = collector.getBoolean("production")

  private val p3p = collector.getConfig("p3p")
  val p3pPolicyRef = p3p.getString("policyref")
  val p3pCP = p3p.getString("CP")

  private val cookie = collector.getConfig("cookie")

  val cookieConfig = if (cookie.getBoolean("enabled")) {
    Some(CookieConfig(
      cookie.getString("name"),
      cookie.getDuration("expiration", TimeUnit.MILLISECONDS),
      cookie.getOptionalString("domain")))
  } else None

  private val sink = collector.getConfig("sink")
  
  // TODO: either change this to ADTs or switch to withName generation
  val sinkEnabled = sink.getString("enabled") match {
    case "kinesis" => SinkType.Kinesis
    case "kafka" => SinkType.Kafka
    case "stdout" => SinkType.Stdout
    case "test" => SinkType.Test
    case _ => throw new RuntimeException("collector.sink.enabled unknown.")
  }

  private val kinesis = sink.getConfig("kinesis")
  private val aws = kinesis.getConfig("aws")
  val awsAccessKey = aws.getString("access-key")
  val awsSecretKey = aws.getString("secret-key")
  private val stream = kinesis.getConfig("stream")
  val streamGoodName = stream.getString("good")
  val streamBadName = stream.getString("bad")
  val streamRegion = stream.getString("region")
  val streamEndpoint = s"https://kinesis.${streamRegion}.amazonaws.com"
  val threadpoolSize = kinesis.hasPath("thread-pool-size") match {
    case true => kinesis.getInt("thread-pool-size")
    case _ => 10
  }
  private val backoffPolicy = kinesis.getConfig("backoffPolicy")
  val minBackoff = backoffPolicy.getLong("minBackoff")
  val maxBackoff = backoffPolicy.getLong("maxBackoff")

  private val kafka = sink.getConfig("kafka")
  val kafkaBrokers = kafka.getString("brokers")
  private val kafkaTopic = kafka.getConfig("topic")
  val kafkaTopicGoodName = kafkaTopic.getString("good")
  val kafkaTopicBadName = kafkaTopic.getString("bad")

  private val buffer = sink.getConfig("buffer")
  val byteLimit = buffer.getInt("byte-limit")
  val recordLimit = buffer.getInt("record-limit")
  val timeLimit = buffer.getLong("time-limit")

  val useIpAddressAsPartitionKey = kinesis.hasPath("useIpAddressAsPartitionKey") && kinesis.getBoolean("useIpAddressAsPartitionKey")

  def cookieName = cookieConfig.map(_.name)
  def cookieDomain = cookieConfig.flatMap(_.domain)
  def cookieExpiration = cookieConfig.map(_.expiration)
}
