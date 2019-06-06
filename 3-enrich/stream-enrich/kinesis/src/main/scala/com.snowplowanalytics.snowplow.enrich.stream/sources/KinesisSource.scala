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

import java.net.InetAddress
import java.util.{List, UUID}

import scala.util.control.Breaks._
import scala.collection.JavaConverters._
import scala.util.control.NonFatal

import cats.Id
import cats.syntax.either._
import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder
import com.amazonaws.services.kinesis.clientlibrary.interfaces._
import com.amazonaws.services.kinesis.clientlibrary.exceptions._
import com.amazonaws.services.kinesis.clientlibrary.lib.worker._
import com.amazonaws.services.kinesis.model.Record
import com.snowplowanalytics.iglu.client.Client
import com.snowplowanalytics.snowplow.badrows.Processor
import com.snowplowanalytics.snowplow.enrich.common.adapters.AdapterRegistry
import com.snowplowanalytics.snowplow.enrich.common.enrichments.EnrichmentRegistry
import com.snowplowanalytics.snowplow.scalatracker.Tracker
import io.circe.Json

import model.{Kinesis, StreamsConfig}
import sinks._

/** KinesisSource companion object with factory method */
object KinesisSource {
  def createAndInitialize(
    config: StreamsConfig,
    client: Client[Id, Json],
    adapterRegistry: AdapterRegistry,
    enrichmentRegistry: EnrichmentRegistry[Id],
    tracker: Option[Tracker[Id]],
    processor: Processor
  ): Either[String, KinesisSource] =
    for {
      kinesisConfig <- config.sourceSink match {
        case c: Kinesis => c.asRight
        case _ => "Configured source/sink is not Kinesis".asLeft
      }
      emitPii = utils.emitPii(enrichmentRegistry)
      _ <- KinesisSink.validate(kinesisConfig, config.out.enriched)
      _ <- utils.validatePii(emitPii, config.out.pii)
      _ <- KinesisSink.validate(kinesisConfig, config.out.bad)
      provider <- KinesisEnrich.getProvider(kinesisConfig.aws)
    } yield new KinesisSource(
      client,
      adapterRegistry,
      enrichmentRegistry,
      tracker,
      processor,
      config,
      kinesisConfig,
      provider
    )
}

/** Source to read events from a Kinesis stream */
class KinesisSource private (
  client: Client[Id, Json],
  adapterRegistry: AdapterRegistry,
  enrichmentRegistry: EnrichmentRegistry[Id],
  tracker: Option[Tracker[Id]],
  processor: Processor,
  config: StreamsConfig,
  kinesisConfig: Kinesis,
  provider: AWSCredentialsProvider
) extends Source(client, adapterRegistry, enrichmentRegistry, processor, config.out.partitionKey) {

  override val MaxRecordSize = Some(1000000)

  private val kClient = {
    val endpointConfiguration =
      new EndpointConfiguration(kinesisConfig.streamEndpoint, kinesisConfig.region)
    AmazonKinesisClientBuilder
      .standard()
      .withCredentials(provider)
      .withEndpointConfiguration(endpointConfiguration)
      .build()
  }

  override val threadLocalGoodSink: ThreadLocal[Sink] = new ThreadLocal[Sink] {
    override def initialValue: Sink =
      new KinesisSink(
        kClient,
        kinesisConfig.backoffPolicy,
        config.buffer,
        config.out.enriched,
        tracker
      )
  }
  override val threadLocalPiiSink: Option[ThreadLocal[Sink]] = {
    val emitPii = utils.emitPii(enrichmentRegistry)
    utils
      .validatePii(emitPii, config.out.pii)
      .toOption
      .flatMap { _ =>
        config.out.pii.map { piiStreamName =>
          new ThreadLocal[Sink] {
            override def initialValue: Sink =
              new KinesisSink(
                kClient,
                kinesisConfig.backoffPolicy,
                config.buffer,
                piiStreamName,
                tracker
              )
          }
        }
      }
  }

  override val threadLocalBadSink: ThreadLocal[Sink] = new ThreadLocal[Sink] {
    override def initialValue: Sink =
      new KinesisSink(kClient, kinesisConfig.backoffPolicy, config.buffer, config.out.bad, tracker)
  }

  /** Never-ending processing loop over source stream. */
  override def run(): Unit = {
    val workerId = InetAddress.getLocalHost().getCanonicalHostName() + ":" + UUID.randomUUID()
    log.info("Using workerId: " + workerId)

    val kinesisClientLibConfiguration = {
      val kclc = new KinesisClientLibConfiguration(
        config.appName,
        config.in.raw,
        provider,
        workerId
      ).withKinesisEndpoint(kinesisConfig.streamEndpoint)
        .withMaxRecords(kinesisConfig.maxRecords)
        .withRegionName(kinesisConfig.region)
        // If the record list is empty, we still check whether it is time to flush the buffer
        .withCallProcessRecordsEvenForEmptyRecordList(true)

      val position = InitialPositionInStream.valueOf(kinesisConfig.initialPosition)
      kinesisConfig.timestamp.right.toOption
        .filter(_ => position == InitialPositionInStream.AT_TIMESTAMP)
        .map(kclc.withTimestampAtInitialPositionInStream(_))
        .getOrElse(kclc.withInitialPositionInStream(position))
    }

    log.info(s"Running: ${config.appName}.")
    log.info(s"Processing raw input stream: ${config.in.raw}")

    val rawEventProcessorFactory = new RawEventProcessorFactory()
    val worker = new Worker.Builder()
      .recordProcessorFactory(rawEventProcessorFactory)
      .config(kinesisClientLibConfiguration)
      .build()

    worker.run()
  }

  // Factory needed by the Amazon Kinesis Consumer library to
  // create a processor.
  class RawEventProcessorFactory extends IRecordProcessorFactory {
    override def createProcessor: IRecordProcessor = new RawEventProcessor()
  }

  // Process events from a Kinesis stream.
  class RawEventProcessor extends IRecordProcessor {
    private var kinesisShardId: String = _

    // Backoff and retry settings.
    private val BACKOFF_TIME_IN_MILLIS = 3000L
    private val NUM_RETRIES = 10

    override def initialize(shardId: String) = {
      log.info("Initializing record processor for shard: " + shardId)
      this.kinesisShardId = shardId
    }

    override def processRecords(
      records: List[Record],
      checkpointer: IRecordProcessorCheckpointer
    ) = {

      if (!records.isEmpty) {
        log.info(s"Processing ${records.size} records from $kinesisShardId")
      }
      val shouldCheckpoint = processRecordsWithRetries(records)

      if (shouldCheckpoint) {
        checkpoint(checkpointer)
      }
    }

    private def processRecordsWithRetries(records: List[Record]): Boolean =
      try {
        enrichAndStoreEvents(records.asScala.map(_.getData.array).toList)
      } catch {
        case NonFatal(e) =>
          // TODO: send an event when something goes wrong here
          log.error(s"Caught throwable while processing records $records", e)
          false
      }

    override def shutdown(checkpointer: IRecordProcessorCheckpointer, reason: ShutdownReason) = {
      log.info(s"Shutting down record processor for shard: $kinesisShardId")
      if (reason == ShutdownReason.TERMINATE) {
        checkpoint(checkpointer)
      }
    }

    private def checkpoint(checkpointer: IRecordProcessorCheckpointer) = {
      log.info(s"Checkpointing shard $kinesisShardId")
      breakable {
        for (i <- 0 to NUM_RETRIES - 1) {
          try {
            checkpointer.checkpoint()
            break
          } catch {
            case se: ShutdownException =>
              log.error("Caught shutdown exception, skipping checkpoint.", se)
              break
            case e: ThrottlingException =>
              if (i >= (NUM_RETRIES - 1)) {
                log.error(s"Checkpoint failed after ${i + 1} attempts.", e)
              } else {
                log.info(
                  s"Transient issue when checkpointing - attempt ${i + 1} of "
                    + NUM_RETRIES,
                  e
                )
              }
            case e: InvalidStateException =>
              log.error(
                "Cannot save checkpoint to the DynamoDB table used by " +
                  "the Amazon Kinesis Client Library.",
                e
              )
              break
          }
          Thread.sleep(BACKOFF_TIME_IN_MILLIS)
        }
      }
    }
  }
}
