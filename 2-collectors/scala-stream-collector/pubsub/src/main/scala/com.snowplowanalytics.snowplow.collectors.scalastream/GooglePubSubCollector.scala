/*
 * Copyright (c) 2013-2017 Snowplow Analytics Ltd. All rights reserved.
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

import scalaz._
import Scalaz._

import model._
import sinks.GooglePubSubSink

object GooglePubSubCollector extends Collector {

  def main(args: Array[String]): Unit = {
    val (collectorConf, akkaConf) = parseConfig(args)

    val sinks = for {
      pc <- collectorConf.streams.sink match {
        case pc: GooglePubSub => pc.right
        case _ => new IllegalArgumentException("Configured sink is not PubSub").left
      }
      goodStream = collectorConf.streams.good
      badStream = collectorConf.streams.bad
      bufferConf = collectorConf.streams.buffer
      good <- GooglePubSubSink.createAndInitialize(pc, bufferConf, goodStream)
      bad <- GooglePubSubSink.createAndInitialize(pc, bufferConf, badStream)
    } yield CollectorSinks(good, bad)

    sinks match {
      case \/-(s) => run(collectorConf, akkaConf, s)
      case -\/(e) => throw e
    }
  }
}
