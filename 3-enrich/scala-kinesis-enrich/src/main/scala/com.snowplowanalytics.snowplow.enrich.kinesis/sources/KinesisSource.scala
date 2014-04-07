 /*
 * Copyright (c) 2013-2014 Snowplow Analytics Ltd.
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
package com.snowplowanalytics.snowplow
package enrich
package kinesis
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
import com.amazonaws.services.kinesis.metrics.impl.NullMetricsFactory
import com.amazonaws.services.kinesis.model.Record

// Scala
import scala.util.control.Breaks._
import scala.collection.JavaConversions._

// Thrift
import org.apache.thrift.TDeserializer

// Snowplow events and enrichment
import sinks._
import collectors.thrift.{
  SnowplowRawEvent,
  TrackerPayload => ThriftTrackerPayload,
  PayloadProtocol,
  PayloadFormat
}

/**
 * Source to read events from a Kinesis stream
 *
 * TODO: replace printlns with using Java logger
 */
class KinesisSource(config: KinesisEnrichConfig)
    extends AbstractSource(config) {
  
  /**
   * Never-ending processing loop over source stream.
   */
  def run {
    val workerId = InetAddress.getLocalHost().getCanonicalHostName() +
      ":" + UUID.randomUUID()
    println("Using workerId: " + workerId)

    val kinesisClientLibConfiguration = new KinesisClientLibConfiguration(
      config.appName,
      config.rawInStream, 
      kinesisProvider,
      workerId
    ).withInitialPositionInStream(
      InitialPositionInStream.valueOf(config.initialPosition)
    )
    
    println(s"Running: ${config.appName}.")
    println(s"Processing raw input stream: ${config.rawInStream}")

    
    val rawEventProcessorFactory = new RawEventProcessorFactory(
      config,
      sink.get // TODO: yech
    )
    val worker = new Worker(
      rawEventProcessorFactory,
      kinesisClientLibConfiguration,
      new NullMetricsFactory()
    )

    worker.run()
  }

  // Factory needed by the Amazon Kinesis Consumer library to
  // create a processor.
  class RawEventProcessorFactory(config: KinesisEnrichConfig, sink: ISink)
      extends IRecordProcessorFactory {
    @Override
    def createProcessor: IRecordProcessor = {
      return new RawEventProcessor(config, sink);
    }
  }

  // Process events from a Kinesis stream.
  class RawEventProcessor(config: KinesisEnrichConfig, sink: ISink)
      extends IRecordProcessor {
    private val thriftDeserializer = new TDeserializer()

    private var kinesisShardId: String = _
    private var nextCheckpointTimeInMillis: Long = _

    // Backoff and retry settings.
    private val BACKOFF_TIME_IN_MILLIS = 3000L
    private val NUM_RETRIES = 10
    private val CHECKPOINT_INTERVAL_MILLIS = 1000L
      
    @Override
    def initialize(shardId: String) = {
      println("Initializing record processor for shard: " + shardId)
      this.kinesisShardId = shardId
    }

    @Override
    def processRecords(records: List[Record],
        checkpointer: IRecordProcessorCheckpointer) = {
      println(s"Processing ${records.size} records from $kinesisShardId")
      processRecordsWithRetries(records)

      if (System.currentTimeMillis() > nextCheckpointTimeInMillis) {
        checkpoint(checkpointer)
        nextCheckpointTimeInMillis =
          System.currentTimeMillis + CHECKPOINT_INTERVAL_MILLIS
      }
    }


    private def processRecordsWithRetries(records: List[Record]) = {
      for (record <- records) {
        try {
          println(s"Sequence number: ${record.getSequenceNumber}")
          println(s"Partition key: ${record.getPartitionKey}")
          enrichEvent(record.getData.array)
        } catch {
          case t: Throwable =>
            println(s"Caught throwable while processing record $record")
            println(t)
        }
      }
    }

    @Override
    def shutdown(checkpointer: IRecordProcessorCheckpointer,
        reason: ShutdownReason) = {
      println(s"Shutting down record processor for shard: $kinesisShardId")
      if (reason == ShutdownReason.TERMINATE) {
        checkpoint(checkpointer)
      }
    }
      
    private def checkpoint(checkpointer: IRecordProcessorCheckpointer) = {
      println(s"Checkpointing shard $kinesisShardId")
      breakable {
        for (i <- 0 to NUM_RETRIES-1) {
          try {
            checkpointer.checkpoint()
            break
          } catch {
            case se: ShutdownException =>
              println("Caught shutdown exception, skipping checkpoint.", se)
            case e: ThrottlingException =>
              if (i >= (NUM_RETRIES - 1)) {
                println(s"Checkpoint failed after ${i+1} attempts.", e)
              } else {
                println(s"Transient issue when checkpointing - attempt ${i+1} of "
                  + NUM_RETRIES, e)
              }
            case e: InvalidStateException =>
              println("Cannot save checkpoint to the DynamoDB table used by " +
                "the Amazon Kinesis Client Library.", e)
          }
          Thread.sleep(BACKOFF_TIME_IN_MILLIS)
        }
      }
    }
  }
}
