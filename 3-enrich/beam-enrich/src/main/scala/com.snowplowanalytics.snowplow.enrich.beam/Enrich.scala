/*
 * Copyright (c) 2012-2019 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.enrich.beam

import cats.Id
import cats.data.{Validated, ValidatedNel}
import cats.implicits._
import com.snowplowanalytics.iglu.client.Client
import com.snowplowanalytics.iglu.core.SelfDescribingData
import com.snowplowanalytics.iglu.core.circe.CirceIgluCodecs._
import com.snowplowanalytics.snowplow.badrows._
import com.snowplowanalytics.snowplow.enrich.common.EtlPipeline
import com.snowplowanalytics.snowplow.enrich.common.adapters.AdapterRegistry
import com.snowplowanalytics.snowplow.enrich.common.enrichments.EnrichmentRegistry
import com.snowplowanalytics.snowplow.enrich.common.enrichments.registry.EnrichmentConf
import com.snowplowanalytics.snowplow.enrich.common.loaders.ThriftLoader
import com.snowplowanalytics.snowplow.enrich.common.outputs.EnrichedEvent
import com.spotify.scio._
import com.spotify.scio.pubsub.PubSubAdmin
import com.spotify.scio.values.{DistCache, SCollection}
import _root_.io.circe.Json
import _root_.io.circe.syntax._
import org.apache.beam.sdk.io.gcp.pubsub.PubsubOptions
import org.joda.time.DateTime
import org.slf4j.LoggerFactory

import config._
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

  val processor = Processor("beam-enrich", generated.BuildInfo.version)

  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)
    val parsedConfig = for {
      config <- EnrichConfig(args)
      _ = sc.setJobName(config.jobName)
      _ <- checkTopicExists(sc, config.enriched)
      _ <- checkTopicExists(sc, config.bad)
      _ <- config.pii.map(checkTopicExists(sc, _)).getOrElse(().asRight)
      resolverJson <- parseResolver(config.resolver)
      client <- Client.parseDefault[Id](resolverJson).leftMap(_.toString).value
      registryJson <- parseEnrichmentRegistry(config.enrichments, client)
      confs <- EnrichmentRegistry.parse(registryJson, client, false).leftMap(_.toString).toEither
      _ <- if (emitPii(confs) && config.pii.isEmpty) {
        "A pii topic needs to be used in order to use the pii enrichment".asLeft
      } else {
        ().asRight
      }
    } yield ParsedEnrichConfig(
      config.raw,
      config.enriched,
      config.bad,
      config.pii,
      resolverJson,
      confs
    )

    parsedConfig match {
      case Left(e) =>
        System.err.println(e)
        System.exit(1)
      case Right(config) =>
        run(sc, config)
        sc.close()
        ()
    }
  }

  def run(sc: ScioContext, config: ParsedEnrichConfig): Unit = {
    val cachedFiles: DistCache[List[Either[String, String]]] =
      buildDistCache(sc, config.enrichmentConfs)

    val raw: SCollection[Array[Byte]] =
      sc.pubsubSubscription[Array[Byte]](config.raw).withName("raw")
    val enriched: SCollection[ValidatedNel[Json, EnrichedEvent]] =
      enrichEvents(raw, config.resolver, config.enrichmentConfs, cachedFiles)

    val (successes, failures) = enriched.partition(_.isValid)
    val (tooBigSuccesses, properlySizedSuccesses) = formatEnrichedEvents(successes)
    properlySizedSuccesses
      .withName("properly-sized-enriched-successes")
      .map(_._1)
      .withName("enriched-good")
      .saveAsPubsub(config.enriched)

    val piis = generatePiiEvents(successes, config.enrichmentConfs)
    (piis, config.pii).mapN { (piis, pii) =>
      val properlySizedPiis = piis._2
      properlySizedPiis
        .withName("properly-sized-pii-successes")
        .map(_._1)
        .withName("pii-good")
        .saveAsPubsub(pii)
    }

    val failureCollection: SCollection[Json] =
      failures
        .collect { case Validated.Invalid(badRows) => badRows.toList }
        .flatten
        .map(resizeBadRow(_, MaxRecordSize, processor))
        .withName("bad-rows") ++
        tooBigSuccesses
          .withName("oversized-enriched-successes")
          .map { case (event, size) => resizeEnrichedEvent(event, size, MaxRecordSize, processor) } ++
        (piis, config.pii)
          .mapN { (piis, _) =>
            val tooBigPiis = piis._1
            tooBigPiis
              .withName("oversized-pii-successes")
              .map {
                case (event, size) => resizeEnrichedEvent(event, size, MaxRecordSize, processor)
              }
          }
          .getOrElse(sc.parallelize(List.empty))
    failureCollection
      .withName("all-bad-rows")
      .map(_.noSpaces)
      .withName("enriched-bad")
      .saveAsPubsub(config.bad)
    ()
  }

  /**
   * Turns a collection of byte arrays into a collection of either bad rows of enriched events.
   * @param raw collection of events
   * @param resolver Json representing the iglu resolver
   * @param enrichmentConfs list of enabled enrichment configuration
   * @param cachedFiles list of files to cache
   */
  private def enrichEvents(
    raw: SCollection[Array[Byte]],
    resolver: Json,
    enrichmentConfs: List[EnrichmentConf],
    cachedFiles: DistCache[List[Either[String, String]]]
  ): SCollection[ValidatedNel[Json, EnrichedEvent]] =
    raw
      .map { rawEvent =>
        cachedFiles()
        val (enriched, time) = timeMs {
          enrich(
            rawEvent,
            EnrichmentRegistrySingleton.get(enrichmentConfs),
            ClientSingleton.get(resolver)
          )
        }
        timeToEnrichDistribution.update(time)
        enriched.map {
          _.leftMap(_.map(br => SelfDescribingData[Json](br.schema, br.data.asJson).asJson))
        }
      }
      .withName("enriched")
      .flatten
      .withName("enriched-flattened")

  /**
   * Turns successfully enriched events into TSV partitioned by whether or no they exceed the
   * maximum size.
   * @param enriched collection of events that went through the enrichment phase
   * @return a collection of properly-sized enriched events and another of oversized ones
   */
  private def formatEnrichedEvents(
    enriched: SCollection[ValidatedNel[Json, EnrichedEvent]]
  ): (SCollection[(String, Int)], SCollection[(String, Int)]) =
    enriched
      .collect {
        case Validated.Valid(enrichedEvent) =>
          getEnrichedEventMetrics(enrichedEvent)
            .foreach(ScioMetrics.counter(MetricsNamespace, _).inc())
          val formattedEnrichedEvent = tabSeparatedEnrichedEvent(enrichedEvent)
          val size = getSize(formattedEnrichedEvent)
          enrichedEventSizeDistribution.update(size.toLong)
          (formattedEnrichedEvent, size)
      }
      .withName("enriched-successes")
      .partition(_._2 >= MaxRecordSize)

  /**
   * Generates PII transformation events depending on the configuration of the PII enrichment.
   * @param enriched collection of events that went through the enrichment phase
   * @param resolver Json representing the iglu resolver
   * @param reigstry Json representing the enrichment registry
   * @return a collection of properly-sized enriched events and another of oversized ones wrapped
   * in an option depending on whether the PII enrichment is configured to emit PII transformation
   * events
   */
  private def generatePiiEvents(
    enriched: SCollection[ValidatedNel[Json, EnrichedEvent]],
    confs: List[EnrichmentConf]
  ): Option[(SCollection[(String, Int)], SCollection[(String, Int)])] =
    if (emitPii(confs)) {
      val (tooBigPiis, properlySizedPiis) = enriched
        .collect {
          case Validated.Valid(enrichedEvent) =>
            getPiiEvent(enrichedEvent)
              .map(tabSeparatedEnrichedEvent)
              .map(formatted => (formatted, getSize(formatted)))
        }
        .flatten
        .withName("pii-successes")
        .partition(_._2 >= MaxRecordSize)
      Some((tooBigPiis, properlySizedPiis))
    } else {
      None
    }

  /**
   * Enrich a collector payload into a list of [[EnrichedEvent]].
   * @param data serialized collector payload
   * @return a list of either [[EnrichedEvent]] or [[BadRow]]
   */
  private def enrich(
    data: Array[Byte],
    enrichmentRegistry: EnrichmentRegistry[Id],
    client: Client[Id, Json]
  ): List[ValidatedNel[SelfDescribingData[BadRow], EnrichedEvent]] = {
    val processor = Processor("beam-enrich", generated.BuildInfo.version)
    val collectorPayload = ThriftLoader.toCollectorPayload(data, processor)
    EtlPipeline.processEvents(
      new AdapterRegistry,
      enrichmentRegistry,
      client,
      processor,
      new DateTime(System.currentTimeMillis),
      collectorPayload
    )
  }

  /**
   * Builds a SCIO's [[DistCache]] which downloads the needed files and create the necessary
   * symlinks.
   * @param sc [[ScioContext]]
   * @param enrichmentConfs list of enrichment configurations
   * @return a properly build [[DistCache]]
   */
  private def buildDistCache(
    sc: ScioContext,
    enrichmentConfs: List[EnrichmentConf]
  ): DistCache[List[Either[String, String]]] = {
    val filesToCache: List[(String, String)] = enrichmentConfs
      .map(_.filesToCache)
      .flatten
      .map { case (uri, sl) => (uri.toString, sl) }
    sc.distCache(filesToCache.map(_._1)) { files =>
      val symLinks = files.toList
        .zip(filesToCache.map(_._2))
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
  private def checkTopicExists(sc: ScioContext, topicName: String): Either[String, Unit] =
    if (sc.isTest) {
      ().asRight
    } else {
      PubSubAdmin.topic(sc.options.as(classOf[PubsubOptions]), topicName) match {
        case scala.util.Success(_) => ().asRight
        case scala.util.Failure(e) =>
          s"Output topic $topicName couldn't be retrieved: ${e.getMessage}".asLeft
      }
    }
}
