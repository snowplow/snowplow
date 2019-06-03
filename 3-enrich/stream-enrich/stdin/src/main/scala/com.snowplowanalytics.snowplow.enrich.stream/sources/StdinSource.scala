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

package com.snowplowanalytics
package snowplow
package enrich
package stream
package sources

import org.apache.commons.codec.binary.Base64
import scalaz._
import Scalaz._
import common.adapters.AdapterRegistry
import common.enrichments.EnrichmentRegistry
import iglu.client.Resolver
import model.{Stdin, StreamsConfig}
import scalatracker.Tracker
import sinks.{Sink, StderrSink, StdoutSink}

/** StdinSource companion object with factory method */
object StdinSource {
  def create(
    config: StreamsConfig,
    igluResolver: Resolver,
    adapterRegistry: AdapterRegistry,
    enrichmentRegistry: EnrichmentRegistry,
    tracker: Option[Tracker]
  ): Validation[String, StdinSource] =
    for {
      _ <- config.sourceSink match {
        case Stdin => ().success
        case _ => "Configured source/sink is not Stdin".failure
      }
    } yield new StdinSource(
      igluResolver,
      adapterRegistry,
      enrichmentRegistry,
      tracker,
      config.out.partitionKey
    )
}

/** Source to decode raw events (in base64) from stdin. */
class StdinSource private (
  igluResolver: Resolver,
  adapterRegistry: AdapterRegistry,
  enrichmentRegistry: EnrichmentRegistry,
  tracker: Option[Tracker],
  partitionKey: String
) extends Source(igluResolver, adapterRegistry, enrichmentRegistry, tracker, partitionKey) {

  override val MaxRecordSize = None

  override val threadLocalGoodSink: ThreadLocal[Sink] = new ThreadLocal[Sink] {
    override def initialValue: Sink = new StdoutSink()
  }
  override val threadLocalPiiSink: Option[ThreadLocal[Sink]] = Some(new ThreadLocal[Sink] {
    override def initialValue: Sink = new StdoutSink()
  })

  override val threadLocalBadSink: ThreadLocal[Sink] = new ThreadLocal[Sink] {
    override def initialValue: Sink = new StderrSink()
  }

  /** Never-ending processing loop over source stream. */
  override def run(): Unit =
    for (ln <- scala.io.Source.stdin.getLines) {
      val bytes = Base64.decodeBase64(ln)
      enrichAndStoreEvents(List(bytes))
    }
}
