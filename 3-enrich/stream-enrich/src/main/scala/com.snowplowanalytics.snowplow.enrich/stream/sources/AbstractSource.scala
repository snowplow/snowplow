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
package com.snowplowanalytics
package snowplow.enrich
package stream 
package sources

// Java
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets.UTF_8
import java.util.UUID

// Amazon
import com.amazonaws.auth._

// Apache commons
import org.apache.commons.codec.binary.Base64

// Scala
import scala.util.Random
import scala.util.control.NonFatal

// Scalaz
import scalaz.{Sink => _, _}
import Scalaz._

// json4s
import org.json4s.scalaz.JsonScalaz._
import org.json4s.{ThreadLocal => _, _}
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

// Joda-Time
import org.joda.time.DateTime

// Iglu
import iglu.client.Resolver
import iglu.client.validation.ProcessingMessageMethods._

// Snowplow
import sinks._
import common.outputs.{
  EnrichedEvent,
  BadRow
}
import common.loaders.ThriftLoader
import common.enrichments.EnrichmentRegistry
import common.enrichments.EnrichmentManager
import common.enrichments.EventEnrichments
import common.adapters.AdapterRegistry

import common.ValidatedMaybeCollectorPayload
import common.EtlPipeline
import common.utils.JsonUtils

// Tracker
import com.snowplowanalytics.snowplow.scalatracker.Tracker

object AbstractSource {

  /**
   * If a bad row JSON is too big, reduce it's size
   *
   * @param value Bad row JSON which is too large
   * @return Bad row JSON with `size` field instead of `line` field
   */
  def adjustOversizedFailureJson(value: String): String = {
    val size = getSize(value)
    try {

      val jsonWithoutLine = parse(value) removeField {
        case ("line", _) => true
        case _ => false
      }

      compact(render({("size" -> size): JValue} merge jsonWithoutLine))

    } catch {
      case NonFatal(e) =>
        BadRow.oversizedRow(size, NonEmptyList("Unable to extract errors field from original oversized bad row JSON"))
    }
  }

  /**
   * Convert a too-large successful event to a failure
   *
   * @param value Event which passed enrichment but was too large
   * @param maximum Maximum allowable bytes
   * @return Bad row JSON
   */
  def oversizedSuccessToFailure(value: String, maximum: Long): String = {
    val size = AbstractSource.getSize(value)
    BadRow.oversizedRow(size, NonEmptyList(s"Enriched event size of $size bytes is greater than allowed maximum of $maximum"))
  }

  /**
   * The size of a string in bytes
   *
   * @param evt
   * @return size
   */
  def getSize(evt: String): Long = ByteBuffer.wrap(evt.getBytes(UTF_8)).capacity
}

/**
 * Abstract base for the different sources
 * we support.
 */
abstract class AbstractSource(config: KinesisEnrichConfig, igluResolver: Resolver,
                              enrichmentRegistry: EnrichmentRegistry, 
                              tracker: Option[Tracker]) {
  
  val MaxRecordSize = if (config.sink == Sink.Kinesis) {
    Some(MaxBytes)
  } else {
    None
  }

  /**
   * Never-ending processing loop over source stream.
   */
  def run

  // Initialize a kinesis provider to use with a Kinesis source or sink.
  protected val kinesisProvider = config.credentialsProvider

  // Initialize the sink to output enriched events to.
  protected val sink = getThreadLocalSink(InputType.Good)

  protected val badSink = getThreadLocalSink(InputType.Bad)

  /**
   * We need the sink to be ThreadLocal as otherwise a single copy
   * will be shared between threads for different shards
   *
   * @param inputType Whether the sink is for good events or bad events
   * @return ThreadLocal sink
   */
  private def getThreadLocalSink(inputType: InputType.InputType) = new ThreadLocal[Option[ISink]] {
    override def initialValue = config.sink match {
      case Sink.Kafka => new KafkaSink(config, inputType, tracker).some
      case Sink.Kinesis => new KinesisSink(kinesisProvider, config, inputType, tracker).some
      case Sink.Stdouterr => new StdouterrSink(inputType).some
      case Sink.Test => None
    }
  }

  implicit val resolver: Resolver = igluResolver

  // Iterate through an enriched EnrichedEvent object and tab separate
  // the fields to a string.
  def tabSeparateEnrichedEvent(output: EnrichedEvent): String = {
    output.getClass.getDeclaredFields
    .map{ field =>
      field.setAccessible(true)
      Option(field.get(output)).getOrElse("")
    }.mkString("\t")
  }

  /**
   * Convert incoming binary Thrift records to lists of enriched events
   *
   * @param binaryData Thrift raw event
   * @return List containing successful or failed events, each with a
   *         partition key
   */
  def enrichEvents(binaryData: Array[Byte]): List[Validation[(String, String), (String, String)]] = {
    val canonicalInput: ValidatedMaybeCollectorPayload = ThriftLoader.toCollectorPayload(binaryData)
    val processedEvents: List[ValidationNel[String, EnrichedEvent]] = EtlPipeline.processEvents(
      enrichmentRegistry,
      s"kinesis-${generated.Settings.version}",
      new DateTime(System.currentTimeMillis),
      canonicalInput)
    processedEvents.map(validatedMaybeEvent => {
      validatedMaybeEvent match {
        case Success(co) => (tabSeparateEnrichedEvent(co), if (config.useIpAddressAsPartitionKey) {
            co.user_ipaddress
          } else {
            UUID.randomUUID.toString
          }).success
        case Failure(errors) => {
          val line = new String(Base64.encodeBase64(binaryData), UTF_8)
          (BadRow(line, errors).toCompactJson -> Random.nextInt.toString).fail
        }
      }
    })
  }

  /**
   * Deserialize and enrich incoming Thrift records and store the results
   * in the appropriate sinks. If doing so causes the number of events
   * stored in a sink to become sufficiently large, all sinks are flushed
   * and we return `true`, signalling that it is time to checkpoint
   *
   * @param binaryData Thrift raw event
   * @return Whether to checkpoint
   */
  def enrichAndStoreEvents(binaryData: List[Array[Byte]]): Boolean = {
    val enrichedEvents = binaryData.flatMap(enrichEvents(_))
    val successes = enrichedEvents collect { case Success(s) => s }
    val sizeUnadjustedFailures = enrichedEvents collect { case Failure(s) => s }
    val failures = sizeUnadjustedFailures map {
      case (value, key) => if (! isTooLarge(value)) {
        value -> key
      } else {
        AbstractSource.adjustOversizedFailureJson(value) -> key
      }
    }

    val (tooBigSuccesses, smallEnoughSuccesses) = successes partition { s => isTooLarge(s._1) }

    val sizeBasedFailures = for {
      (value, key) <- tooBigSuccesses
      m <- MaxRecordSize
    } yield AbstractSource.oversizedSuccessToFailure(value, m) -> key

    val successesTriggeredFlush = sink.get.map(_.storeEnrichedEvents(smallEnoughSuccesses))
    val failuresTriggeredFlush = badSink.get.map(_.storeEnrichedEvents(failures ++ sizeBasedFailures))
    if (successesTriggeredFlush == Some(true) || failuresTriggeredFlush == Some(true)) {

      // Block until the records have been sent to Kinesis
      sink.get.foreach(_.flush)
      badSink.get.foreach(_.flush)
      true
    } else {
      false
    }

  }

  /**
   * Whether a record is too large to send to Kinesis
   *
   * @param evt
   * @return boolean size decision
   */
  private def isTooLarge(evt: String): Boolean = MaxRecordSize match {
    case None => false
    case Some(m) => AbstractSource.getSize(evt) >= m
  }

}
