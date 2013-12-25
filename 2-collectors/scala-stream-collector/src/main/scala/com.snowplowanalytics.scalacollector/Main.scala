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
import org.rogach.scallop.exceptions.ScallopException
import spray.can.Http

// Config
import com.typesafe.config.ConfigFactory

// Grab all the configuration variables one-time
object CollectorConfig {
  private val config = ConfigFactory.load("application")
  private val collector = config.getConfig("collector")
  val interface = collector.getString("interface")
  val port = collector.getInt("port")

  private val aws = collector.getConfig("aws")
  val awsAccessKey = aws.getString("access-key")
  val awsSecretKey = aws.getString("secret-key")

  private val stream = collector.getConfig("stream")
  val streamName = stream.getString("name")
  val streamSize = stream.getInt("size")
}

object ScalaCollector extends App {
  implicit val system = ActorSystem()

  // The handler actor replies to incoming HttpRequests.
  val handler = system.actorOf(Props[CollectorServiceActor], name = "handler")

  IO(Http) ! Http.Bind(handler,
    interface=CollectorConfig.interface, port=CollectorConfig.port)
}
