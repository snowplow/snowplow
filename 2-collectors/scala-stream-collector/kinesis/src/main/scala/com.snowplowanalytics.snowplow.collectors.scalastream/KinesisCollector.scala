/*
 * Copyright (c) 2013-2019 Snowplow Analytics Ltd. All rights reserved.
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

import cats.syntax.either._

import model._
import sinks.KinesisSink

object KinesisCollector extends Collector {

  def main(args: Array[String]): Unit = {
    val (collectorConf, akkaConf) = parseConfig(args)

    val sinks: Either[Throwable, CollectorSinks] = for {
      kc <- collectorConf.streams.sink match {
        case kc: Kinesis => kc.asRight
        case _ => new IllegalArgumentException("Configured sink is not Kinesis").asLeft
      }
      es = new ScheduledThreadPoolExecutor(kc.threadPoolSize)
      goodStream = collectorConf.streams.good
      badStream = collectorConf.streams.bad
      bufferConf = collectorConf.streams.buffer
      good <- KinesisSink.createAndInitialize(kc, bufferConf, goodStream, es)
      bad <- KinesisSink.createAndInitialize(kc, bufferConf, badStream, es)
    } yield CollectorSinks(good, bad)

    sinks match {
      case Right(s) => run(collectorConf, akkaConf, s)
      case Left(e) => throw e
    }
  }
}
