 /*
 * Copyright (c) 2013-2016 Snowplow Analytics Ltd.
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

// Java
import java.io.{FileInputStream,IOException}
import java.net.InetAddress
import java.nio.ByteBuffer
import java.util.{List,UUID}

// Amazon
import com.amazonaws.auth._
import com.amazonaws.AmazonClientException
import com.amazonaws.services.kinesis.AmazonKinesisClient
import com.amazonaws.services.kinesis.clientlibrary.interfaces._
import com.amazonaws.services.kinesis.clientlibrary.exceptions._
import com.amazonaws.services.kinesis.clientlibrary.lib.worker._
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownReason
import com.amazonaws.services.kinesis.model.Record

// Logging
import org.slf4j.LoggerFactory

// Scala
import scala.util.control.Breaks._
import scala.collection.JavaConversions._
import scala.util.control.NonFatal

// Thrift
import org.apache.thrift.TDeserializer

// Iglu
import iglu.client.Resolver

// Snowplow events and enrichment
import common.enrichments.EnrichmentRegistry
import sinks._
import collectors.thrift.{
  SnowplowRawEvent,
  TrackerPayload => ThriftTrackerPayload,
  PayloadProtocol,
  PayloadFormat
}

// Tracker
import com.snowplowanalytics.snowplow.scalatracker.Tracker

/**
 * Source to read events from a Kinesis stream
 */
class KinesisSource(config: KinesisEnrichConfig, igluResolver: Resolver, enrichmentRegistry: EnrichmentRegistry, tracker: Option[Tracker])
    extends AbstractSource(config, igluResolver, enrichmentRegistry, tracker) {
  
  lazy val log = LoggerFactory.getLogger(getClass())
  import log.{error, debug, info, trace}

  /**
   * Never-ending processing loop over source stream.
   */
  def run {
    val workerId = InetAddress.getLocalHost().getCanonicalHostName() +
      ":" + UUID.randomUUID()
    info("Using workerId: " + workerId)

    val kinesisClientLibConfiguration = new KinesisClientLibConfiguration(
      config.appName,
      config.rawInStream, 
      kinesisProvider,
      workerId
    ).withInitialPositionInStream(
      InitialPositionInStream.valueOf(config.initialPosition)
    ).withKinesisEndpoint(config.streamEndpoint)
    .withMaxRecords(config.maxRecords)
    .withRegionName(config.streamRegion)
    // If the record list is empty, we still check whether it is time to flush the buffer
    .withCallProcessRecordsEvenForEmptyRecordList(true)

    info(s"Running: ${config.appName}.")
    info(s"Processing raw input stream: ${config.rawInStream}")

    val rawEventProcessorFactory = new RawEventProcessorFactory(
      config,
      sink.get.get // TODO: yech, yech
    )
    val worker = new Worker(
      rawEventProcessorFactory,
      kinesisClientLibConfiguration
    )

    worker.run()
  }

  // Factory needed by the Amazon Kinesis Consumer library to
  // create a processor.
  class RawEventProcessorFactory(config: KinesisEnrichConfig, sink: ISink)
      extends IRecordProcessorFactory {
    @Override
    def createProcessor: IRecordProcessor = {
      new RawEventProcessor(config, sink);
    }
  }

  // Process events from a Kinesis stream.
  class RawEventProcessor(config: KinesisEnrichConfig, sink: ISink)
      extends IRecordProcessor {
    private val thriftDeserializer = new TDeserializer()

    private var kinesisShardId: String = _

    // Backoff and retry settings.
    private val BACKOFF_TIME_IN_MILLIS = 3000L
    private val NUM_RETRIES = 10
    private val CHECKPOINT_INTERVAL_MILLIS = 1000L
      
    @Override
    def initialize(shardId: String) = {
      info("Initializing record processor for shard: " + shardId)
      this.kinesisShardId = shardId
    }

    @Override
    def processRecords(records: List[Record],
        checkpointer: IRecordProcessorCheckpointer) = {

      if (!records.isEmpty) {
        info(s"Processing ${records.size} records from $kinesisShardId")
      }
      val shouldCheckpoint = processRecordsWithRetries(records)

      if (shouldCheckpoint) {
        checkpoint(checkpointer)
      }
    }

    private def processRecordsWithRetries(records: List[Record]): Boolean = {
      try {
        enrichAndStoreEvents(records.map(_.getData.array).toList)
      } catch {
        case NonFatal(e) =>
          // TODO: send an event when something goes wrong here
          error(s"Caught throwable while processing records $records", e)
          false
      }
    }

    @Override
    def shutdown(checkpointer: IRecordProcessorCheckpointer,
        reason: ShutdownReason) = {
      info(s"Shutting down record processor for shard: $kinesisShardId")
      if (reason == ShutdownReason.TERMINATE) {
        checkpoint(checkpointer)
      }
    }

    private def checkpoint(checkpointer: IRecordProcessorCheckpointer) = {
      info(s"Checkpointing shard $kinesisShardId")
      breakable {
        for (i <- 0 to NUM_RETRIES-1) {
          try {
            checkpointer.checkpoint()
            break
          } catch {
            case se: ShutdownException =>
              error("Caught shutdown exception, skipping checkpoint.", se)
              break
            case e: ThrottlingException =>
              if (i >= (NUM_RETRIES - 1)) {
                error(s"Checkpoint failed after ${i+1} attempts.", e)
              } else {
                info(s"Transient issue when checkpointing - attempt ${i+1} of "
                  + NUM_RETRIES, e)
              }
            case e: InvalidStateException =>
              error("Cannot save checkpoint to the DynamoDB table used by " +
                "the Amazon Kinesis Client Library.", e)
              break
          }
          Thread.sleep(BACKOFF_TIME_IN_MILLIS)
        }
      }
    }
  }
}
