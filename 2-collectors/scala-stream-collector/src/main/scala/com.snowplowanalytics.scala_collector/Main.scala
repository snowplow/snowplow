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

package com.snowplowanalytics.scala_collector

import akka.actor.{ActorSystem, Props}
import akka.io.IO
import spray.can.Http

import org.rogach.scallop.exceptions.ScallopException

object ScalaCollector extends App {
  implicit val system = ActorSystem()

  val conf = new ScalaCollectorConf(args)

  // The handler actor replies to incoming HttpRequests.
  val handler = system.actorOf(Props[CollectorServiceActor], name = "handler")

  IO(Http) ! Http.Bind(handler,
    interface=conf.interface.apply(), port=conf.port.apply())
}
