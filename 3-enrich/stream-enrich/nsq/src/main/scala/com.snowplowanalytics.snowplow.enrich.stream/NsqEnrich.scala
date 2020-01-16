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

import java.io.File
import java.net.URI

import cats.Id
import cats.syntax.either._
import com.snowplowanalytics.snowplow.enrich.common.adapters.AdapterRegistry
import com.snowplowanalytics.snowplow.enrich.common.enrichments.EnrichmentRegistry
import com.snowplowanalytics.snowplow.badrows.Processor
import com.snowplowanalytics.iglu.client.Client
import com.snowplowanalytics.snowplow.scalatracker.Tracker
import io.circe.Json

import config.FileConfig
import model.{Credentials, StreamsConfig}
import sources.NsqSource

/** The main entry point for Stream Enrich for NSQ. */
object NsqEnrich extends Enrich {

  def main(args: Array[String]): Unit = run(args)

  def getSource(
    streamsConfig: StreamsConfig,
    client: Client[Id, Json],
    adapterRegistry: AdapterRegistry,
    enrichmentRegistry: EnrichmentRegistry[Id],
    tracker: Option[Tracker[Id]],
    processor: Processor
  ): Either[String, sources.Source] =
    NsqSource
      .create(streamsConfig, client, adapterRegistry, enrichmentRegistry, processor)
      .leftMap(_.getMessage)

  override val parser: scopt.OptionParser[FileConfig] = localParser

  override def download(
    uri: URI,
    targetFile: File
  )(
    implicit creds: Credentials
  ): Either[String, Int] =
    httpDownloader(uri, targetFile)

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
