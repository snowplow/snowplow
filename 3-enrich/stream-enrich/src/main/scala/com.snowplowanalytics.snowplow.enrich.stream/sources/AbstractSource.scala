/*
 * Copyright (c) 2013-2017 Snowplow Analytics Ltd.
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

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets.UTF_8
import java.util.UUID

import scala.util.Random
import scala.util.control.NonFatal

import org.apache.commons.codec.binary.Base64
import scalaz.{Sink => _, _}
import Scalaz._
import org.json4s.{ThreadLocal => _, _}
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import org.joda.time.DateTime

import common.{EtlPipeline, ValidatedMaybeCollectorPayload}
import common.enrichments.EnrichmentRegistry
import common.loaders.ThriftLoader
import common.outputs.{EnrichedEvent, BadRow}
import iglu.client.Resolver
import model._
import scalatracker.Tracker
import sinks._

object AbstractSource {
  /** Kinesis records must not exceed 1MB */
  val MaxBytes = 1000000L

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
  def getSize(evt: String): Long = ByteBuffer.wrap(evt.getBytes(UTF_8)).capacity.toLong
}

/**
 * Abstract base for the different sources
 * we support.
 */
abstract class AbstractSource(
  config: EnrichConfig,
  igluResolver: Resolver,
  enrichmentRegistry: EnrichmentRegistry,
  tracker: Option[Tracker]
) {

  val MaxRecordSize = if (config.sinkType == KinesisSink) {
    Some(AbstractSource.MaxBytes)
  } else {
    None
  }

  /**
   * Never-ending processing loop over source stream.
   */
  def run(): Unit

  // Initialize a kinesis provider to use with a Kinesis source or sink.
  protected val kinesisProvider = config.aws.provider

  // Initialize the sink to output enriched events to.
  protected val sink = getThreadLocalSink(Good)

  protected val badSink = getThreadLocalSink(Bad)

  private def getStreamName(inputType: InputType): String = inputType match {
    case Good => config.streams.out.enriched
    case Bad => config.streams.out.bad
  }

  /**
   * We need the sink to be ThreadLocal as otherwise a single copy
   * will be shared between threads for different shards
   *
   * @param inputType Whether the sink is for good events or bad events
   * @return ThreadLocal sink
   */
  private def getThreadLocalSink(inputType: InputType) = new ThreadLocal[Option[ISink]] {
    val streamName = getStreamName(inputType)
    lazy val kafkaConfig = config.streams.kafka
    lazy val kinesisConfig = config.streams.kinesis
    lazy val nsqConfig = config.streams.nsq
    val bufferConfig = config.streams.buffer
    override def initialValue = config.sinkType match {
      case KafkaSink =>
        new KafkaSink(kafkaConfig, bufferConfig, inputType, streamName, tracker).some
      case KinesisSink => new KinesisSink(kinesisProvider, kinesisConfig, bufferConfig, inputType,
        streamName, tracker).some
      case StdouterrSink => new StdouterrSink(inputType).some
      case NsqSink => new NsqSink(nsqConfig, streamName).some
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

  def getProprertyValue(ee: EnrichedEvent, property: String): String =
    property match {
      case "event_id" => ee.event_id
      case "event_fingerprint" => ee.event_fingerprint
      case "domain_userid" => ee.domain_userid
      case "network_userid" => ee.network_userid
      case "user_ipaddress" => ee.user_ipaddress
      case "domain_sessionid" => ee.domain_sessionid
      case "user_fingerprint" => ee.user_fingerprint
      case _ => UUID.randomUUID().toString
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
        case Success(co) =>
          (tabSeparateEnrichedEvent(co), getProprertyValue(co, config.streams.out.partitionKey)).success
        case Failure(errors) =>
          val line = new String(Base64.encodeBase64(binaryData), UTF_8)
          (BadRow(line, errors).toCompactJson -> Random.nextInt.toString).fail
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
