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
import akka.actor.{ ActorSystem, Props }
import akka.io.IO
import spray.can.Http

// Java
import java.io.File

// Argot
import org.clapper.argot._

// Config
import com.typesafe.config.{ ConfigFactory, Config, ConfigException }

// Logging
import org.slf4j.LoggerFactory

// Snowplow
import sinks._

// Main entry point of the Scala collector.
object ScalaCollector extends App with CollectorConfig {
  lazy val log = LoggerFactory.getLogger(getClass())
  import log.{ error, debug, info, trace }
  import ArgotConverters._ // Argument specifications

  val parser = new ArgotParser(
    programName = generated.Settings.name,
    compactUsage = true,
    preUsage = Some("%s: Version %s. Copyright (c) 2013, %s.".format(
      generated.Settings.name,
      generated.Settings.version,
      generated.Settings.organization)))

  // Mandatory config argument
  val configArg = parser.option[Config](List("config"), "filename",
    "Configuration file.") { (c, opt) =>
      val file = new File(c)
      if (file.exists) {
        ConfigFactory.parseFile(file)
      } else {
        parser.usage("Configuration file \"%s\" does not exist".format(c))
        ConfigFactory.empty()
      }
    }
  parser.parse(args)

  val rawConf = configArg.value.getOrElse(throw new RuntimeException("--config option must be provided"))
  implicit val system = ActorSystem.create("scala-stream-collector", rawConf)
  override val config = rawConf

  // The handler actor replies to incoming HttpRequests.
  val handler = system.actorOf(CollectorHandler.props(this, sink), name = "handler")

  IO(Http) ! Http.Bind(handler, interface, port)
}

/*
 Return Options from the configuration.
 */
object Helper {
  implicit class RichConfig(val underlying: Config) extends AnyVal {
    def getOptionalString(path: String): Option[String] = try {
      Some(underlying.getString(path))
    } catch {
      case e: ConfigException.Missing => None
    }
  }
}

object CollectorConfig {
  val DEFAULT_THREAD_POOL_SIZE = 10
  def endpoint(region: String): String = s"https://kinesis.${region}.amazonaws.com"
}

/*
 Rigidly load the configuration file here to error when
 the collector process starts rather than later.
*/
trait CollectorConfig {
  import Helper.RichConfig
  import CollectorConfig._

  protected val config: Config

  private val collector = config.getConfig("collector")
  protected[scalastream] val interface = collector.getString("interface")
  protected[scalastream] val port = collector.getInt("port")
  protected[scalastream] val production = collector.getBoolean("production")

  private val p3p = collector.getConfig("p3p")
  protected[scalastream] val p3pPolicyRef = p3p.getString("policyref")
  protected[scalastream] val p3pCP = p3p.getString("CP")

  private val cookie = collector.getConfig("cookie")
  protected[scalastream] val cookieExpiration = cookie.getMilliseconds("expiration")
  protected[scalastream] val cookieDomain = cookie.getOptionalString("domain")

  private val sinkConfig = collector.getConfig("sink")
  val sinkEnabled = sinkConfig.getString("enabled").toLowerCase

  protected[scalastream] val sink = sinkEnabled match {
    case "kinesis" => new KinesisSink(this)
    case "stdout"  => new StdoutSink
    case "test"    => new TestSink
    case _         => throw new RuntimeException("collector.sink.enabled.unknown.")
  }

  private val kinesis = sinkConfig.getConfig("kinesis")
  private val aws = kinesis.getConfig("aws")
  private val stream = kinesis.getConfig("stream")
  private val streamRegion = stream.getString("region")

  //public api
  val awsAccessKey = aws.getString("access-key")
  val awsSecretKey = aws.getString("secret-key") // public api
  val streamName = stream.getString("name")
  val streamSize = stream.getInt("size")
  val streamEndpoint = endpoint(streamRegion)
  val threadpoolSize = kinesis.hasPath("thread-pool-size") match {
    case true => kinesis.getInt("thread-pool-size")
    case _    => DEFAULT_THREAD_POOL_SIZE
  }
}
