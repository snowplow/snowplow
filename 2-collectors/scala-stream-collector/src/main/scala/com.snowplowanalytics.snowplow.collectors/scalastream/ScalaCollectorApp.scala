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

// Akka and Spray
import akka.actor.{ActorSystem, Props}
import akka.pattern.ask
import akka.io.IO
import spray.can.Http

// Scala Futures
import scala.concurrent.duration._
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Success, Failure}

// Java
import java.io.File
import java.nio.ByteBuffer
import java.util.concurrent.ScheduledThreadPoolExecutor
import java.util.concurrent.TimeUnit

// Config
import com.typesafe.config.{ConfigFactory,Config,ConfigException}

// Logging
import org.slf4j.LoggerFactory

// Snowplow
import sinks._

// Main entry point of the Scala collector.
object ScalaCollector extends App {
  lazy val log = LoggerFactory.getLogger(getClass())
  import log.{error, debug, info, trace}

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

  lazy val executorService = new ScheduledThreadPoolExecutor(collectorConfig.threadpoolSize)

  val sinks = collectorConfig.sinkEnabled match {
    case Sink.Kinesis => {
      val good = KinesisSink.createAndInitialize(collectorConfig, InputType.Good, executorService)
      val bad  = KinesisSink.createAndInitialize(collectorConfig, InputType.Bad, executorService)
      CollectorSinks(good, bad) 
    }
    case Sink.Kafka => {
      val good = new KafkaSink(collectorConfig, InputType.Good)
      val bad  = new KafkaSink(collectorConfig, InputType.Bad)
      CollectorSinks(good, bad)
    }
    case Sink.Stdout  => {
      val good = new StdoutSink(InputType.Good)
      val bad = new StdoutSink(InputType.Bad)
      CollectorSinks(good, bad) 
    }
  }

  // The handler actor replies to incoming HttpRequests.
  val handler = system.actorOf(
    Props(classOf[CollectorServiceActor], collectorConfig, sinks),
    name = "handler"
  )

  val bind = Http.Bind(
    handler,
    interface=collectorConfig.interface,
    port=collectorConfig.port)

  val bindResult = IO(Http).ask(bind)(5.seconds) flatMap {
    case b: Http.Bound => Future.successful(())
    case failed: Http.CommandFailed => Future.failed(new RuntimeException(failed.toString))
  }

  bindResult onComplete {
    case Success(_) =>
    case Failure(f) => {
      error("Failure binding to port", f)
      System.exit(1)
    }
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

// Instead of comparing strings and validating every time
// the sink is accessed, validate the string here and
// store this enumeration.
object Sink extends Enumeration {
  type Sink = Value
  val Kinesis, Kafka, Stdout, Test = Value
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
    case "kinesis" => Sink.Kinesis
    case "kafka" => Sink.Kafka
    case "stdout" => Sink.Stdout
    case "test" => Sink.Test
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
  val timeLimit = buffer.getInt("time-limit")

  val useIpAddressAsPartitionKey = kinesis.hasPath("useIpAddressAsPartitionKey") && kinesis.getBoolean("useIpAddressAsPartitionKey")

  def cookieName = cookieConfig.map(_.name)
  def cookieDomain = cookieConfig.flatMap(_.domain)
  def cookieExpiration = cookieConfig.map(_.expiration)
}
