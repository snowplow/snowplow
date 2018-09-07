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

import java.nio.charset.StandardCharsets.UTF_8

import com.spotify.scio._
import com.spotify.scio.pubsub.PubSubAdmin
import com.spotify.scio.values.{DistCache, SCollection}
import org.apache.beam.sdk.io.gcp.pubsub.PubsubOptions
import org.apache.commons.codec.binary.Base64
import org.joda.time.DateTime
import org.json4s.{JObject, JValue}
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

/** Enrich job using the Beam API through SCIO */
object Enrich {

  private val logger = LoggerFactory.getLogger(this.getClass)
  // the maximum record size in Google PubSub is 10Mb
  private val MaxRecordSize = 10000000
  private val MetricsNamespace = "snowplow"

  val enrichedEventSizeDistribution =
    ScioMetrics.distribution(MetricsNamespace, "enriched_event_size_bytes")
  val timeToEnrichDistribution =
    ScioMetrics.distribution(MetricsNamespace, "time_to_enrich_ms")

  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)
    val parsedConfig = for {
      config <- EnrichConfig(args)
      _ = sc.setJobName(config.jobName)
      _ <- checkTopicExists(sc, config.enriched)
      _ <- checkTopicExists(sc, config.bad)
      _ <- config.pii.map(checkTopicExists(sc, _)).getOrElse(().success)
      resolverJson <- parseResolver(config.resolver)
      resolver <- Resolver.parse(resolverJson).leftMap(_.toList.mkString("\n"))
      registryJson <- parseEnrichmentRegistry(config.enrichments)(resolver)
      registry <- EnrichmentRegistry.parse(registryJson, false)(resolver).leftMap(_.toList.mkString("\n"))
      _ <- if (emitPii(registry) && config.pii.isEmpty) {
        "A pii topic needs to be used in order to use the pii enrichment".failure
      } else {
        ().success
      }
    } yield ParsedEnrichConfig(
      config.raw, config.enriched, config.bad, config.pii, resolverJson, registryJson)

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
    val cachedFiles: DistCache[List[Either[String, String]]] =
      buildDistCache(sc, config.resolver, config.enrichmentRegistry)

    val raw: SCollection[Array[Byte]] = sc.pubsubSubscription(config.raw).withName("raw")
    val enriched: SCollection[Validation[BadRow, EnrichedEvent]] =
      enrichEvents(raw, config.resolver, config.enrichmentRegistry, cachedFiles)

    val (successes, failures) = enriched.partition(_.isSuccess)
    val (tooBigSuccesses, properlySizedSuccesses) = formatEnrichedEvents(successes)
    properlySizedSuccesses.withName("properly-sized-enriched-successes")
      .map(_._1).withName("enriched-good")
      .saveAsPubsub(config.enriched)

    val piis = generatePiiEvents(successes, config.resolver, config.enrichmentRegistry)
    (piis |@| config.pii) { (piis, pii) =>
      val properlySizedPiis = piis._2
      properlySizedPiis.withName("properly-sized-pii-successes")
        .map(_._1).withName("pii-good")
        .saveAsPubsub(pii)
    }

    val failureCollection: SCollection[BadRow] =
      failures.collect { case Failure(badRow) => resizeBadRow(badRow, MaxRecordSize) }
        .withName("bad-rows") ++
      tooBigSuccesses.withName("oversized-enriched-successes")
        .map { case (event, size) => resizeEnrichedEvent(event, size, MaxRecordSize) } ++
      (piis |@| config.pii) { (piis, pii) =>
        val tooBigPiis = piis._1
        tooBigPiis.withName("oversized-pii-successes")
          .map { case (event, size) => resizeEnrichedEvent(event, size, MaxRecordSize) }
      }.getOrElse(sc.parallelize(List.empty))
    failureCollection.withName("all-bad-rows")
      .map(_.toCompactJson).withName("enriched-bad")
      .saveAsPubsub(config.bad)
  }

  /**
   * Turns a collection of byte arrays into a collection of either bad rows of enriched events.
   * @param raw collection of events
   * @param resolver JValue needed to build the resolver
   * @param enrichmentRegistry JObject needed to build the resolver
   * @param cachedFiles list of files to cache
   */
  private def enrichEvents(
    raw: SCollection[Array[Byte]],
    resolver: JValue,
    enrichmentRegistry: JObject,
    cachedFiles: DistCache[List[Either[String, String]]]
  ): SCollection[Validation[BadRow, EnrichedEvent]]= raw
    .map { rawEvent =>
      cachedFiles()
      implicit val r = ResolverSingleton.get(resolver)
      val (enriched, time) = timeMs {
        enrich(rawEvent, EnrichmentRegistrySingleton.get(enrichmentRegistry))
      }
      timeToEnrichDistribution.update(time)
      enriched
    }.withName("enriched")
    .flatten.withName("enriched-flattened")

  /**
   * Turns successfully enriched events into TSV partitioned by whether or no they exceed the
   * maximum size.
   * @param enriched collection of events that went through the enrichment phase
   * @return a collection of properly-sized enriched events and another of oversized ones
   */
  private def formatEnrichedEvents(
    enriched: SCollection[Validation[BadRow, EnrichedEvent]]
  ): (SCollection[(String, Int)], SCollection[(String, Int)]) = enriched
    .collect { case Success(enrichedEvent) =>
      getEnrichedEventMetrics(enrichedEvent)
        .foreach(ScioMetrics.counter(MetricsNamespace, _).inc())
      val formattedEnrichedEvent = tabSeparatedEnrichedEvent(enrichedEvent)
      val size = getStringSize(formattedEnrichedEvent)
      enrichedEventSizeDistribution.update(size.toLong)
      (formattedEnrichedEvent, size)
    }.withName("enriched-successes")
    .partition(_._2 >= MaxRecordSize)

  /**
   * Generates PII transformation events depending on the configuration of the PII enrichment.
   * @param enriched collection of events that went through the enrichment phase
   * @param resolver JValue needed to build the resolver
   * @param enrichmentRegistry JObject needed to build the resolver
   * @return a collection of properly-sized enriched events and another of oversized ones wrapped
   * in an option depending on whether the PII enrichment is configured to emit PII transformation
   * events
   */
  private def generatePiiEvents(
    enriched: SCollection[Validation[BadRow, EnrichedEvent]],
    resolver: JValue,
    enrichmentRegistry: JObject
  ): Option[(SCollection[(String, Int)], SCollection[(String, Int)])] = {
    implicit val r = ResolverSingleton.get(resolver)
    val registry = EnrichmentRegistrySingleton.get(enrichmentRegistry)
    if (emitPii(registry)) {
      val (tooBigPiis, properlySizedPiis) = enriched
        .collect { case Success(enrichedEvent) =>
          getPiiEvent(enrichedEvent)
            .map(tabSeparatedEnrichedEvent)
            .map(formatted => (formatted, getStringSize(formatted)))
        }
        .flatten.withName("pii-successes")
        .partition(_._2 >= MaxRecordSize)
      Some((tooBigPiis, properlySizedPiis))
    } else {
      None
    }
  }

  /**
   * Enrich a collector payload into a list of [[EnrichedEvent]].
   * @param data serialized collector payload
   * @return a list of either [[EnrichedEvent]] or [[BadRow]]
   */
  private def enrich(data: Array[Byte], enrichmentRegistry: EnrichmentRegistry)(
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

  /**
   * Builds a SCIO's [[DistCache]] which downloads the needed files and create the necessary
   * symlinks.
   * @param sc [[ScioContext]]
   * @param resolver JValue needed to build the resolver
   * @param enrichmentRegistry JObject needed to build the resolver
   * @return a properly build [[DistCache]]
   */
  private def buildDistCache(
    sc: ScioContext,
    resolver: JValue,
    enrichmentRegistry: JObject
  ): DistCache[List[Either[String, String]]] = {
    val filesToCache = getFilesToCache(resolver, enrichmentRegistry)
    // Path is not serializable
    sc.distCache(filesToCache.map(_._1.toString)) { files =>
      val symLinks = files.toList.zip(filesToCache.map(_._2))
        .map { case (file, symLink) => createSymLink(file, symLink) }
      symLinks.zip(files).foreach {
        case (Right(p), file) => logger.info(s"File $file cached at $p")
        case (Left(e), file) => logger.warn(s"File $file could not be cached: $e")
      }
      symLinks.map(_.map(_.toString))
    }
  }

  /**
   * Checks a PubSub topic exists before launching the job.
   * @param sc [[ScioContext]]
   * @param topicName name of the topic to check for existence, projects/{project}/topics/{topic}
   * @return Right if it exists, left otherwise
   */
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
