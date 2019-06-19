/*
 * Copyright (c) 2013-2019 Snowplow Analytics Ltd.
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

package com.snowplowanalytics.snowplow.enrich.stream
package sources

import cats.Id
import cats.syntax.either._
import com.snowplowanalytics.client.nsq._
import com.snowplowanalytics.client.nsq.callbacks._
import com.snowplowanalytics.client.nsq.exceptions.NSQException
import com.snowplowanalytics.client.nsq.lookup.DefaultNSQLookup
import com.snowplowanalytics.iglu.client.Client
import com.snowplowanalytics.snowplow.badrows.Processor
import com.snowplowanalytics.snowplow.enrich.common.adapters.AdapterRegistry
import com.snowplowanalytics.snowplow.enrich.common.enrichments.EnrichmentRegistry
import io.circe.Json

import model.{Nsq, StreamsConfig}
import sinks.{NsqSink, Sink}

/** NsqSource companion object with factory method */
object NsqSource {
  def create(
    config: StreamsConfig,
    client: Client[Id, Json],
    adapterRegistry: AdapterRegistry,
    enrichmentRegistry: EnrichmentRegistry[Id],
    processor: Processor
  ): Either[Throwable, NsqSource] =
    for {
      nsqConfig <- config.sourceSink match {
        case c: Nsq => c.asRight
        case _ => new IllegalArgumentException("Configured source/sink is not Nsq").asLeft
      }
      goodProducer <- NsqSink.validateAndCreateProducer(nsqConfig)
      emitPii = utils.emitPii(enrichmentRegistry)
      _ <- utils
        .validatePii(emitPii, config.out.pii)
        .leftMap(new IllegalArgumentException(_))
      piiProducer <- config.out.pii match {
        case Some(_) => NsqSink.validateAndCreateProducer(nsqConfig).map(Some(_))
        case None => None.asRight
      }
      badProducer <- NsqSink.validateAndCreateProducer(nsqConfig)
    } yield new NsqSource(
      goodProducer,
      piiProducer,
      badProducer,
      client,
      adapterRegistry,
      enrichmentRegistry,
      processor,
      config,
      nsqConfig
    )
}

/** Source to read raw events from NSQ. */
class NsqSource private (
  goodProducer: NSQProducer,
  piiProducer: Option[NSQProducer],
  badProducer: NSQProducer,
  client: Client[Id, Json],
  adapterRegistry: AdapterRegistry,
  enrichmentRegistry: EnrichmentRegistry[Id],
  processor: Processor,
  config: StreamsConfig,
  nsqConfig: Nsq
) extends Source(client, adapterRegistry, enrichmentRegistry, processor, config.out.partitionKey) {

  override val MaxRecordSize = None

  override val threadLocalGoodSink: ThreadLocal[Sink] = new ThreadLocal[Sink] {
    override def initialValue: Sink = new NsqSink(goodProducer, config.out.enriched)
  }

  override val threadLocalPiiSink: Option[ThreadLocal[Sink]] = piiProducer.flatMap {
    somePiiProducer =>
      config.out.pii.map { piiTopicName =>
        new ThreadLocal[Sink] {
          override def initialValue: Sink = new NsqSink(somePiiProducer, piiTopicName)
        }
      }
  }

  override val threadLocalBadSink: ThreadLocal[Sink] = new ThreadLocal[Sink] {
    override def initialValue: Sink = new NsqSink(badProducer, config.out.bad)
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
      lookup,
      config.in.raw,
      nsqConfig.rawChannel,
      nsqCallback,
      new NSQConfig(),
      errorCallback
    )
    consumer.start()
    ()
  }
}
