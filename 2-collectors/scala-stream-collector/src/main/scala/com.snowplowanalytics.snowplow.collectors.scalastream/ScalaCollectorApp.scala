/* 
 * Copyright (c) 2013-2014 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.collectors.scalastream

// Akka and Spray
import akka.actor.{ActorSystem, Props}
import akka.io.IO
import spray.can.Http

// Java
import java.io.File

// Argot
import org.clapper.argot._

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

  import ArgotConverters._ // Argument specifications

  val parser = new ArgotParser(
    programName = generated.Settings.name,
    compactUsage = true,
    preUsage = Some("%s: Version %s. Copyright (c) 2013, %s.".format(
      generated.Settings.name,
      generated.Settings.version,
      generated.Settings.organization)
    )
  )

  // Optional config argument
  val config = parser.option[Config](List("config"), "filename",
    "Configuration file. Defaults to \"resources/application.conf\" " +
      "(within .jar) if not set") { (c, opt) =>
    val file = new File(c)
    if (file.exists) {
      ConfigFactory.parseFile(file)
    } else {
      parser.usage("Configuration file \"%s\" does not exist".format(c))
      ConfigFactory.empty()
    }
  }
  parser.parse(args)

  val rawConf = config.value.getOrElse(ConfigFactory.load("application"))
  implicit val system = ActorSystem.create("scala-stream-collector", rawConf)
  val collectorConfig = new CollectorConfig(rawConf)
  val sink = collectorConfig.sinkEnabled match {
    case Sink.Kinesis => new KinesisSink(collectorConfig)
    case Sink.Stdout => new StdoutSink
  }

  // The handler actor replies to incoming HttpRequests.
  val handler = system.actorOf(
    Props(classOf[CollectorServiceActor], collectorConfig, sink),
    name = "handler"
  )

  IO(Http) ! Http.Bind(handler,
    interface=collectorConfig.interface, port=collectorConfig.port)
}
// Return Options from the configuration.
object Helper {
  implicit class RichConfig(val underlying: Config) extends AnyVal {
    def getOptionalString(path: String): Option[String] = try {
      Some(underlying.getString(path))
    } catch {
      case e: ConfigException.Missing => None
    }
  }
}

// Instead of comparing strings and validating every time
// the sink is accessed, validate the string here and
// store this enumeration.
object Sink extends Enumeration {
  type Sink = Value
  val Kinesis, Stdout, Test = Value
}

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
  val cookieExpiration = cookie.getMilliseconds("expiration")
  var cookieDomain = cookie.getOptionalString("domain")

  private val sink = collector.getConfig("sink")
  // TODO: either change this to ADTs or switch to withName generation
  val sinkEnabled = sink.getString("enabled") match {
    case "kinesis" => Sink.Kinesis
    case "stdout" => Sink.Stdout
    case "test" => Sink.Test
    case _ => throw new RuntimeException("collector.sink.enabled unknown.")
  }

  private val kinesis = sink.getConfig("kinesis")
  private val aws = kinesis.getConfig("aws")
  val awsAccessKey = aws.getString("access-key")
  val awsSecretKey = aws.getString("secret-key")
  private val stream = kinesis.getConfig("stream")
  val streamName = stream.getString("name")
  val streamSize = stream.getInt("size")
}

