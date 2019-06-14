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

import model._
import sinks.NsqSink

object NsqCollector extends Collector {
  def main(args: Array[String]): Unit = {
    val (collectorConf, akkaConf) = parseConfig(args)
    val sinks = {
      val goodStream = collectorConf.streams.good
      val badStream = collectorConf.streams.bad
      val (good, bad) = collectorConf.streams.sink match {
        case nc: Nsq => (new NsqSink(nc, goodStream), new NsqSink(nc, badStream))
        case _ => throw new IllegalArgumentException("Configured sink is not NSQ")
      }
      CollectorSinks(good, bad)
    }
    run(collectorConf, akkaConf, sinks)
  }
}
