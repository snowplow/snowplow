/*
 * Copyright (c) 2012-2018 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at
 * http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and
 * limitations there under.
 */
package com.snowplowanalytics
package snowplow
package enrich
package beam

import java.io.File
import java.net.URI
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path, Paths}

import scala.util.Try

import com.spotify.scio._
import com.spotify.scio.pubsub.PubSubAdmin
import com.spotify.scio.values.{DistCache, SCollection}
import org.apache.beam.sdk.io.gcp.pubsub.PubsubOptions
import org.apache.commons.codec.binary.Base64
import org.json4s.{JObject, JValue}
import org.joda.time.DateTime
import org.slf4j.LoggerFactory
import scalaz._
import Scalaz._

import common.EtlPipeline
import common.enrichments.EnrichmentRegistry
import common.loaders.ThriftLoader
import common.outputs.{EnrichedEvent, BadRow}
import config._
import iglu.client.Resolver
import singleton._
import utils._

/*
sbt "runMain com.snowplowanalytics.snowplow.enrich.beam.Enrich
  --project=[PROJECT] --runner=DataflowRunner --zone=[ZONE] --streaming=true
  --job-name=[JOB NAME]
  --input=[INPUT SUBSCRIPTION]
  --output=[OUTPUT TOPIC]
  --bad=[BAD TOPIC]
  --resolver=[RESOLVER FILE PATH]
  --enrichments=[ENRICHMENTS DIR PATH]"
*/
object Enrich {

  private val logger = LoggerFactory.getLogger(this.getClass)
  // the maximum record size in Google PubSub is 10Mb
  private val MaxRecordSize = 10000000

  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)
    val parsedConfig = for {
      config <- EnrichConfig(args)
      _ = sc.setJobName(config.jobName)
      _ <- checkTopicExists(sc, config.output)
      _ <- checkTopicExists(sc, config.bad)
      resolverJson <- parseResolver(config.resolver)
      resolver <- Resolver.parse(resolverJson).leftMap(_.toList.mkString("\n"))
      enrichmentRegistryJson <- parseEnrichmentRegistry(config.enrichments)(resolver)
    } yield ParsedEnrichConfig(
      config.input, config.output, config.bad, resolverJson, enrichmentRegistryJson)

    parsedConfig match {
      case Failure(e) =>
        System.err.println(e)
        System.exit(1)
      case Success(config) =>
        run(sc, config)
        sc.close()
    }
  }

  def run(sc: ScioContext, config: ParsedEnrichConfig): Unit = {
    // Path is not serializable
    val cachedFiles: DistCache[List[Either[String, String]]] = {
      val filesToCache = getFilesToCache(config.resolver, config.enrichmentRegistry)
      sc.distCache(filesToCache.map(_._1.toString)) { files =>
        createSymLinks(files.toList.zip(filesToCache.map(_._2))).map(_.map(_.toString))
      }
    }

    val input: SCollection[Array[Byte]] = sc.pubsubSubscription(config.input).withName("input")
    val enriched: SCollection[Validation[BadRow, EnrichedEvent]] = input
      .map { rawEvent =>
        cachedFiles()
        implicit val resolver = ResolverSingleton.get(config.resolver)
        enrich(rawEvent, EnrichmentRegistrySingleton.get(config.enrichmentRegistry))
      }
      .flatten
      .withName("enriched")

    val (successes, failures) = enriched.partition(_.isSuccess)
    val (tooBigSuccesses, properlySizedsuccesses) = successes
      .collect { case Success(enrichedEvent) =>
        val formattedEnrichedEvent = tabSeparatedEnrichedEvent(enrichedEvent)
        (formattedEnrichedEvent, getStringSize(formattedEnrichedEvent))
      }
      .partition(_._2 >= MaxRecordSize)
    properlySizedsuccesses.map(_._1).withName("enriched-good").saveAsPubsub(config.output)

    val failureCollection: SCollection[BadRow] =
      failures.collect { case Failure(badRow) => resizeBadRow(badRow, MaxRecordSize) } ++
      tooBigSuccesses.map { case (event, size) => resizeEnrichedEvent(event, size, MaxRecordSize) }
    failureCollection.map(_.toCompactJson).withName("enriched-bad").saveAsPubsub(config.bad)
  }

  def enrich(data: Array[Byte], enrichmentRegistry: EnrichmentRegistry)(
      implicit r: Resolver): List[Validation[BadRow, EnrichedEvent]] = {
    val collectorPayload = ThriftLoader.toCollectorPayload(data)
    val processedEvents = EtlPipeline.processEvents(
      enrichmentRegistry,
      s"beam-enrich-${generated.BuildInfo.version}",
      new DateTime(System.currentTimeMillis),
      collectorPayload
    )
    processedEvents.map {
      case Success(enrichedEvent) => enrichedEvent.success
      case Failure(errors) =>
        val line = new String(Base64.encodeBase64(data), UTF_8)
        BadRow(line, errors).failure
    }
  }

  def createSymLinks(filesToCache: List[(File, String)]): List[Either[String, Path]] = filesToCache
    .map { case (file, symLink) =>
      val link = createSymLink(file, symLink)
      link match {
        case Right(p) => logger.info(s"File $file cached at $p")
        case Left(e) => logger.warn(s"File $file could not be cached: $e")
      }
      link
    }

  def createSymLink(file: File, symLink: String): Either[String, Path] = {
    val symLinkPath = Paths.get(symLink)
    if (Files.notExists(symLinkPath)) {
      Try(Files.createSymbolicLink(symLinkPath, file.toPath)) match {
        case scala.util.Success(p) => Right(p)
        case scala.util.Failure(t) => Left(t.getMessage)
      }
    } else Left(s"Symlink $symLinkPath already exists")
  }

  def getFilesToCache(resolverJson: JValue, registryJson: JObject): List[(URI, String)] = {
    implicit val resolver = ResolverSingleton.get(resolverJson)
    val registry = EnrichmentRegistrySingleton.get(registryJson)
    registry.getIpLookupsEnrichment.map(_.dbsToCache).getOrElse(Nil)
  }

  private def checkTopicExists(sc: ScioContext, topicName: String): Validation[String, Unit] =
    if (sc.isTest) {
      ().success
    } else {
      PubSubAdmin.topic(sc.options.as(classOf[PubsubOptions]), topicName) match {
        case scala.util.Success(_) => ().success
        case scala.util.Failure(e) =>
          s"Output topic $topicName couldn't be retrieved: ${e.getMessage}".failure
      }
    }
}
