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

package com.snowplowanalytics.scalacollector

import akka.actor.{ActorSystem, Props}
import akka.io.IO
import spray.can.Http

// Config
import com.typesafe.config.{ConfigFactory,Config,ConfigException}

// Logging.
import org.slf4j.LoggerFactory

// Grab all the configuration variables one-time.
// Some are 'var' for the test suite to update on the fly.
object CollectorConfig {
  // Return Options from the configuration.
  implicit class RichConfig(val underlying: Config) extends AnyVal {
    def getOptionalString(path: String): Option[String] = try {
      Some(underlying.getString(path))
    } catch {
      case e: ConfigException.Missing => None
    }
  }
  private val config = ConfigFactory.load("application")
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

  private val backend = collector.getConfig("backend")
  val backendEnabled = backend.getString("enabled")

  private val kinesis = backend.getConfig("kinesis")
  private val aws = kinesis.getConfig("aws")
  val awsAccessKey = aws.getString("access-key")
  val awsSecretKey = aws.getString("secret-key")
  private val stream = kinesis.getConfig("stream")
  val streamName = stream.getString("name")
  val streamSize = stream.getInt("size")

  private val stdout = backend.getConfig("stdout")
  val stdoutDelimiter = stdout.getInt("delimiter")
}

object ScalaCollector extends App {
  lazy val log = LoggerFactory.getLogger(getClass())
  import log.{error, debug, info, trace}

  if (!KinesisInterface.createAndLoadStream()) {
    info("Error initializing or connecting to the stream.")
  } else {
    implicit val system = ActorSystem()

    // The handler actor replies to incoming HttpRequests.
    val handler = system.actorOf(Props[CollectorServiceActor], name = "handler")

    IO(Http) ! Http.Bind(handler,
      interface=CollectorConfig.interface, port=CollectorConfig.port)
  }
}
