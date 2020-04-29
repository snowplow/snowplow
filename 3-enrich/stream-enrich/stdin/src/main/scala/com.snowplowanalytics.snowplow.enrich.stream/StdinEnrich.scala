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

import cats.Id
import com.snowplowanalytics.iglu.client.Client
import com.snowplowanalytics.snowplow.badrows.Processor
import com.snowplowanalytics.snowplow.enrich.common.adapters.AdapterRegistry
import com.snowplowanalytics.snowplow.enrich.common.enrichments.EnrichmentRegistry
import com.snowplowanalytics.snowplow.scalatracker.Tracker
import io.circe.Json

import config.FileConfig
import model.{Credentials, StreamsConfig}
import sources.{Source, StdinSource}

/** The main entry point for Stream Enrich for stdin/out. */
object StdinEnrich extends Enrich {

  def main(args: Array[String]): Unit = run(args)

  override def getSource(
    streamsConfig: StreamsConfig,
    client: Client[Id, Json],
    adapterRegistry: AdapterRegistry,
    enrichmentRegistry: EnrichmentRegistry[Id],
    tracker: Option[Tracker[Id]],
    processor: Processor
  ): Either[String, Source] =
    StdinSource.create(streamsConfig, client, adapterRegistry, enrichmentRegistry, processor)

  override val parser: scopt.OptionParser[FileConfig] = localParser

  override def extractResolver(
    resolverArgument: String
  )(
    implicit creds: Credentials
  ): Either[String, String] =
    localResolverExtractor(resolverArgument)

  override def extractEnrichmentConfigs(
    enrichmentArg: Option[String]
  )(
    implicit creds: Credentials
  ): Either[String, Json] =
    localEnrichmentConfigsExtractor(enrichmentArg)
}
