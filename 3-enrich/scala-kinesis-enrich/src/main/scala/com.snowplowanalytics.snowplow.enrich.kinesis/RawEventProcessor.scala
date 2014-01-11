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
package enrich.kinesis

// Snowplow events and enrichment.
import com.snowplowanalytics.snowplow.collectors.thrift.{
  SnowplowRawEvent,
  TrackerPayload => ThriftTrackerPayload,
  PayloadProtocol,
  PayloadFormat
}
//import enrich.common.inputs.{CanonicalInput,ThriftLoader}
import enrich.common.inputs._ // TODO: Remove
import enrich.common.MaybeCanonicalInput
import enrich.common.outputs.CanonicalOutput
import enrich.common.enrichments.EnrichmentManager
import enrich.common.enrichments.PrivacyEnrichments.AnonQuartets

// Scala MaxMind GeoIP
import com.snowplowanalytics.maxmind.geoip.IpGeo

// Java.
import java.util.List
import java.nio.ByteBuffer

// Amazon.
import com.amazonaws.services.kinesis.clientlibrary.exceptions.{
  InvalidStateException,
  ShutdownException,
  ThrottlingException
}
import com.amazonaws.services.kinesis.clientlibrary.interfaces.{
  IRecordProcessor,
  IRecordProcessorFactory,
  IRecordProcessorCheckpointer
}
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownReason
import com.amazonaws.services.kinesis.model.Record

// Scala.
import scala.util.control.Breaks._
import scala.collection.JavaConversions._

// Thrift.
import org.apache.thrift.TDeserializer

class RawEventProcessorFactory(config: KinesisEnrichConfig,
    kinesisEnrichedSink: KinesisSink) extends IRecordProcessorFactory {
  @Override
  def createProcessor: IRecordProcessor = {
    return new RawEventProcessor(config, kinesisEnrichedSink);
  }
}

class RawEventProcessor(config: KinesisEnrichConfig,
    kinesisEnrichedSink: KinesisSink) extends IRecordProcessor {
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

  private def tabSeparateCanonicalOutput(output: CanonicalOutput): String = {
    output.getClass.getDeclaredFields.map{ field =>
      field.setAccessible(true)
      field.get(output)
    }.mkString("\t")
  }

  private def enrichEvent(binaryData: Array[Byte]) = {
    val canonicalInput = ThriftLoader.toCanonicalInput(
      new String(binaryData.map(_.toChar))
    )

    (canonicalInput.toValidationNel) map { (ci: MaybeCanonicalInput) =>
      if (ci.isDefined) {
        val ipGeo = new IpGeo(
          dbFile = config.maxmindFile,
          memCache = false,
          lruCache = 20000
        )
        val anonQuartets =
          if (!config.anonIpEnabled || config.anonQuartets == 0) {
            AnonQuartets.None
          } else {
            AnonQuartets(config.anonQuartets)
          }
        val canonicalOutput = EnrichmentManager.enrichEvent(
          ipGeo,
          s"kinesis-${generated.Settings.version}",
          anonQuartets,
          ci.get
        )
        (canonicalOutput.toValidationNel) map { (co: CanonicalOutput) =>
          kinesisEnrichedSink.storeEnrichedEvent(
            tabSeparateCanonicalOutput(co).getBytes,
            co.user_ipaddress
          )
          // TODO: Store bad event if canonical output not validated.
        }
      } else {
        // TODO: Store bad event if canonical input is None.
      }
      // TODO: Store bad event if canonical input not validated.
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
