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

import org.apache.commons.codec.binary.Base64

import cats.Id
import cats.syntax.either._
import com.snowplowanalytics.iglu.client.Client
import com.snowplowanalytics.snowplow.badrows.Processor
import com.snowplowanalytics.snowplow.enrich.common.adapters.AdapterRegistry
import com.snowplowanalytics.snowplow.enrich.common.enrichments.EnrichmentRegistry
import io.circe.Json

import model.{Stdin, StreamsConfig}
import sinks.{Sink, StderrSink, StdoutSink}

/** StdinSource companion object with factory method */
object StdinSource {
  def create(
    config: StreamsConfig,
    client: Client[Id, Json],
    adapterRegistry: AdapterRegistry,
    enrichmentRegistry: EnrichmentRegistry[Id],
    processor: Processor
  ): Either[String, StdinSource] =
    for {
      _ <- config.sourceSink match {
        case Stdin => ().asRight
        case _ => "Configured source/sink is not Stdin".asLeft
      }
    } yield new StdinSource(
      client,
      adapterRegistry,
      enrichmentRegistry,
      processor,
      config.out.partitionKey
    )
}

/** Source to decode raw events (in base64) from stdin. */
class StdinSource private (
  client: Client[Id, Json],
  adapterRegistry: AdapterRegistry,
  enrichmentRegistry: EnrichmentRegistry[Id],
  processor: Processor,
  partitionKey: String
) extends Source(client, adapterRegistry, enrichmentRegistry, processor, partitionKey) {

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
