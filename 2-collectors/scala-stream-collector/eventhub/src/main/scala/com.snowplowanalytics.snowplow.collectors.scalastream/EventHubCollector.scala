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
package com.snowplowanalytics.snowplow.collectors.scalastream

import java.util.concurrent.ScheduledThreadPoolExecutor

import scalaz._
import Scalaz._

import model._
import sinks.EventHubSink

object EventHubCollector extends Collector {

  def main(args: Array[String]): Unit = {
    val (collectorConf, akkaConf) = parseConfig(args)

    val sinks = for {
      ehConf <- collectorConf.streams.sink match {
        case eh: EventHub => eh.right
        case _ => new IllegalArgumentException("Configured sink is not EventHub").left
      }
      es = new ScheduledThreadPoolExecutor(ehConf.threadPoolSize)
      goodStream = collectorConf.streams.good
      badStream = collectorConf.streams.bad
      good <- EventHubSink.createAndInitialize(
        ehConf,
        goodStream,
        ehConf.domainName,
        ehConf.eventHubKeys.goodAccessKeyName,
        ehConf.eventHubKeys.goodAccessKey,
        es)
      bad <- EventHubSink.createAndInitialize(
        ehConf,
        badStream,
        ehConf.domainName,
        ehConf.eventHubKeys.badAccessKeyName,
        ehConf.eventHubKeys.badAccessKey,
        es)
    } yield CollectorSinks(good, bad)

    sinks match {
      case \/-(s) => run(collectorConf, akkaConf, s)
      case -\/(e) => throw e
    }
  }
}
