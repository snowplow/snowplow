/*
* Copyright (c) 2013-2018 Snowplow Analytics Ltd.
* All rights reserved.
*
* This program is licensed to you under the Apache License Version 2.0,
* and you may not use this file except in compliance with the Apache
* License Version 2.0.
* You may obtain a copy of the Apache License Version 2.0 at
* http://www.apache.org/licenses/LICENSE-2.0.
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the Apache License Version 2.0 is distributed
* on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
* either express or implied.
*
* See the Apache License Version 2.0 for the specific language
* governing permissions and limitations there under.
*/

package com.snowplowanalytics
package snowplow
package enrich
package stream
package sources

import com.snowplowanalytics.client.nsq.NSQConsumer
import com.snowplowanalytics.client.nsq.lookup.DefaultNSQLookup
import com.snowplowanalytics.client.nsq.NSQMessage
import com.snowplowanalytics.client.nsq.NSQConfig
import com.snowplowanalytics.client.nsq.callbacks.NSQMessageCallback
import com.snowplowanalytics.client.nsq.callbacks.NSQErrorCallback
import com.snowplowanalytics.client.nsq.exceptions.NSQException
import scalaz._
import Scalaz._

import iglu.client.Resolver
import common.enrichments.EnrichmentRegistry
import model.{Nsq, StreamsConfig}
import sinks.{NsqSink, Sink}
import scalatracker.Tracker

/** NsqSource companion object with factory method */
object NsqSource {
  def create(
    config: StreamsConfig,
    igluResolver: Resolver,
    enrichmentRegistry: EnrichmentRegistry,
    tracker: Option[Tracker]
  ): Validation[Throwable, NsqSource] = for {
    nsqConfig <- config.sourceSink match {
      case c: Nsq => c.success
      case _ => new IllegalArgumentException("Configured source/sink is not Nsq").failure
    }
  } yield new NsqSource(igluResolver, enrichmentRegistry, tracker, config, nsqConfig)
}

/** Source to read raw events from NSQ. */
class NsqSource private (
  igluResolver: Resolver,
  enrichmentRegistry: EnrichmentRegistry,
  tracker: Option[Tracker],
  config: StreamsConfig,
  nsqConfig: Nsq
) extends Source(igluResolver, enrichmentRegistry, tracker, config.out.partitionKey) {

  override val MaxRecordSize = None

  override val threadLocalGoodSink: ThreadLocal[Sink] = new ThreadLocal[Sink] {
    override def initialValue: Sink = new NsqSink(nsqConfig, config.out.enriched)
  }
  override val threadLocalBadSink: ThreadLocal[Sink] = new ThreadLocal[Sink] {
    override def initialValue: Sink = new NsqSink(nsqConfig, config.out.bad)
  }

  /** Consumer will be started to wait new message. */
  override def run(): Unit = {

    val nsqCallback = new NSQMessageCallback {
      override def message(msg: NSQMessage): Unit = {
        val bytes = msg.getMessage()
        enrichAndStoreEvents(List(bytes)) match {
          case true => msg.finished()
          case false => log.error(s"Error while enriching the event")
        }
      }
    }

    val errorCallback = new NSQErrorCallback {
      override def error(e: NSQException): Unit =
        log.error(s"Exception while consuming topic ${config.in.raw}", e)
    }

    // use NSQLookupd
    val lookup = new DefaultNSQLookup
    lookup.addLookupAddress(nsqConfig.lookupHost, nsqConfig.lookupPort)
    val consumer = new NSQConsumer(
      lookup, config.in.raw, nsqConfig.rawChannel, nsqCallback, new NSQConfig(), errorCallback)
    consumer.start()
  }
}
