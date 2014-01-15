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

// Snowplow events and enrichment
import sinks._
import collectors.thrift.{
  SnowplowRawEvent,
  TrackerPayload => ThriftTrackerPayload,
  PayloadProtocol,
  PayloadFormat
}
import common.MaybeCanonicalInput
import common.inputs.ThriftLoader
import common.outputs.CanonicalOutput
import common.enrichments.EnrichmentManager
import common.enrichments.PrivacyEnrichments.AnonOctets

// Scala MaxMind GeoIP
import com.snowplowanalytics.maxmind.geoip.IpGeo

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


class KinesisSource(kinesisEnrichConfig: KinesisEnrichConfig) {
  def run {
    val workerId = InetAddress.getLocalHost().getCanonicalHostName() +
      ":" + UUID.randomUUID()
    println("Using workerId: " + workerId)

    val kinesisProvider = createKinesisProvider(
      kinesisEnrichConfig.accessKey,
      kinesisEnrichConfig.secretKey
    )
    val kinesisClientLibConfiguration = new KinesisClientLibConfiguration(
      kinesisEnrichConfig.appName,
      kinesisEnrichConfig.rawInStream, 
      kinesisProvider,
      workerId
    ).withInitialPositionInStream(
      InitialPositionInStream.valueOf(kinesisEnrichConfig.initialPosition)
    )
    
    println(s"Running: ${kinesisEnrichConfig.appName}.")
    println(s"Processing raw input stream: ${kinesisEnrichConfig.rawInStream}")

    val kinesisEnrichedSink = new KinesisSink(kinesisProvider)
    val successful = kinesisEnrichedSink.createAndLoadStream(
      kinesisEnrichConfig.enrichedOutStream,
      kinesisEnrichConfig.enrichedOutStreamShards
    )
    if (!successful) {
      println("Error initializing or connecting to the stream.")
      sys.exit(-1)
    }
    
    val rawEventProcessorFactory = new RawEventProcessorFactory(
      kinesisEnrichConfig,
      kinesisEnrichedSink
    )
    val worker = new Worker(
      rawEventProcessorFactory,
      kinesisClientLibConfiguration,
      new NullMetricsFactory()
    )

    worker.run()
  }

  def runTest {
    throw new RuntimeException("Unimplemented.")
  }

  private def createKinesisProvider(accessKey: String, secretKey: String):
      AWSCredentialsProvider =
    if (isCpf(accessKey) && isCpf(secretKey)) {
        new ClasspathPropertiesFileCredentialsProvider()
    } else if (isCpf(accessKey) || isCpf(secretKey)) {
      throw new RuntimeException(
        "access-key and secret-key must both be set to 'cpf', or neither"
      )
    } else {
      new BasicAWSCredentialsProvider(
        new BasicAWSCredentials(accessKey, secretKey)
      )
    }
  private def isCpf(key: String): Boolean = (key == "cpf")

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
        Option(field.get(output)).getOrElse("")
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
          val anonOctets =
            if (!config.anonIpEnabled || config.anonOctets == 0) {
              AnonOctets.None
            } else {
              AnonOctets(config.anonOctets)
            }
          val canonicalOutput = EnrichmentManager.enrichEvent(
            ipGeo,
            s"kinesis-${generated.Settings.version}",
            anonOctets,
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
          // CanonicalInput is None: do nothing
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


  class BasicAWSCredentialsProvider(basic: BasicAWSCredentials) extends
      AWSCredentialsProvider{
    @Override def getCredentials: AWSCredentials = basic
    @Override def refresh = {}
  }
}
