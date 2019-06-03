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

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets.UTF_8
import java.time.Instant
import java.util.UUID

import scala.util.Random

import cats.Id
import cats.data.ValidatedNel
import cats.data.Validated.{Invalid, Valid}
import cats.syntax.option._
import cats.syntax.validated._
import com.snowplowanalytics.iglu.client.Client
import com.snowplowanalytics.snowplow.badrows._
import com.snowplowanalytics.snowplow.enrich.common.EtlPipeline
import com.snowplowanalytics.snowplow.enrich.common.adapters.AdapterRegistry
import com.snowplowanalytics.snowplow.enrich.common.enrichments.EnrichmentRegistry
import com.snowplowanalytics.snowplow.enrich.common.loaders.{CollectorPayload, ThriftLoader}
import com.snowplowanalytics.snowplow.enrich.common.outputs.EnrichedEvent
import io.circe.Json
import io.circe.syntax._
import org.joda.time.DateTime
import org.slf4j.LoggerFactory
import org.joda.time.{DateTime, DateTimeZone}
import org.joda.time.format.DateTimeFormat

import sinks._
import utils._

object Source {
  val processor = Processor("stream-enrich", "1.0.0") // TODO generate through code

  val PiiEventName = "pii_transformation"
  val PiiEventVendor = "com.snowplowanalytics.snowplow"
  val PiiEventFormat = "jsonschema"
  val PiiEventVersion = "1-0-0"
  val PiiEventPlatform = "srv"
  val ContextsSchema = "iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-0"
  val ParentEventSchema = "iglu:com.snowplowanalytics.snowplow/parent_event/jsonschema/1-0-0"

  /**
   * If a bad row JSON is too big, reduce it's size
   * @param value Bad row which is too large
   * @param maxSizeBytes maximum size in bytes
   * @param processor current processor
   * @return Bad row embedding truncated bad row
   */
  def adjustOversizedFailureJson(
    badRow: BadRow,
    maxSizeBytes: Int,
    processor: Processor
  ): BadRow.SizeViolation = {
    val size = getSizeBr(badRow)
    BadRow.SizeViolation(
      processor,
      Failure.SizeViolation(Instant.now(), maxSizeBytes, size, "bad row exceeded the maximum size"),
      Payload.RawPayload(brToString(badRow).take(maxSizeBytes / 10))
    )
  }

  /**
   * Convert a too-large successful event to a failure
   * @param value Event which passed enrichment but was too large
   * @param maxSizeBytes maximum size in bytes
   * @param processor current processor
   * @return Bad row JSON
   */
  def oversizedSuccessToFailure(
    value: String,
    maxSizeBytes: Int,
    processor: Processor
  ): BadRow.SizeViolation = {
    val size = getSize(value)
    val msg = "event passed enrichment but exceeded the maximum allowed size as a result"
    BadRow.SizeViolation(
      processor,
      Failure.SizeViolation(Instant.now(), maxSizeBytes, size, msg),
      Payload.RawPayload(value.take(maxSizeBytes / 10))
    )
  }

  val brToString: BadRow => String = br => br.compact

  /** The size of a string in bytes */
  val getSize: String => Int = evt => ByteBuffer.wrap(evt.getBytes(UTF_8)).capacity

  /** The size of a bad row in bytes */
  val getSizeBr: BadRow => Int =
    (brToString andThen getSize)(_)
}

/** Abstract base for the different sources we support. */
abstract class Source(
  client: Client[Id, Json],
  adapterRegistry: AdapterRegistry,
  enrichmentRegistry: EnrichmentRegistry[Id],
  processor: Processor,
  partitionKey: String
) {

  val MaxRecordSize: Option[Int]

  lazy val log = LoggerFactory.getLogger(getClass())

  /** Never-ending processing loop over source stream. */
  def run(): Unit

  private val formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS")

  def getPiiEvent(event: EnrichedEvent): Option[EnrichedEvent] =
    Option(event.pii)
      .filter(_.nonEmpty)
      .map { _ =>
        val ee = new EnrichedEvent
        ee.unstruct_event = event.pii
        ee.app_id = event.app_id
        ee.platform = Source.PiiEventPlatform
        ee.etl_tstamp = event.etl_tstamp
        ee.collector_tstamp = event.collector_tstamp
        ee.event = Source.PiiEventName
        ee.event_id = UUID.randomUUID().toString
        ee.derived_tstamp = formatter.print(DateTime.now(DateTimeZone.UTC))
        ee.true_tstamp = ee.derived_tstamp
        ee.event_vendor = Source.PiiEventVendor
        ee.event_format = Source.PiiEventFormat
        ee.event_name = Source.PiiEventName
        ee.event_version = Source.PiiEventVersion
        ee.contexts = getContextParentEvent(event.event_id).noSpaces
        ee.v_etl =
          s"stream-enrich-${generated.BuildInfo.version}-common-${generated.BuildInfo.commonEnrichVersion}"
        ee
      }

  def getContextParentEvent(eventId: String): Json =
    Json.obj(
      "schema" := Json.fromString(Source.ContextsSchema),
      "data" := Json.arr(
        Json.obj(
          "schema" -> Json.fromString(Source.ParentEventSchema),
          "data" -> Json.obj("parentEventId" -> Json.fromString(eventId))
        )
      )
    )

  val threadLocalGoodSink: ThreadLocal[Sink]
  val threadLocalPiiSink: Option[ThreadLocal[Sink]]
  val threadLocalBadSink: ThreadLocal[Sink]

  // Iterate through an enriched EnrichedEvent object and tab separate
  // the fields to a string.
  def tabSeparateEnrichedEvent(output: EnrichedEvent): String =
    output.getClass.getDeclaredFields
      .filterNot(_.getName.equals("pii"))
      .map { field =>
        field.setAccessible(true)
        Option(field.get(output)).getOrElse("")
      }
      .mkString("\t")

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
   * @param binaryData Thrift raw event
   * @return List containing failed, successful and, if present, pii events. Successful and failed, each specify a
   *         partition key.
   */
  def enrichEvents(
    binaryData: Array[Byte]
  ): List[ValidatedNel[(BadRow, String), (String, String, Option[String])]] = {
    val canonicalInput: ValidatedNel[BadRow, Option[CollectorPayload]] =
      ThriftLoader.toCollectorPayload(binaryData, processor)
    val processedEvents: List[ValidatedNel[BadRow, EnrichedEvent]] =
      EtlPipeline.processEvents(
        adapterRegistry,
        enrichmentRegistry,
        client,
        Processor("stream-enrich", generated.BuildInfo.version),
        new DateTime(System.currentTimeMillis),
        canonicalInput
      )
    processedEvents.map {
      case Valid(ee) =>
        (
          tabSeparateEnrichedEvent(ee),
          getProprertyValue(ee, partitionKey),
          getPiiEvent(ee).map(tabSeparateEnrichedEvent)
        ).valid
      case Invalid(nel) => nel.map(_ -> Random.nextInt().toString()).invalid
    }
  }

  /**
   * Deserialize and enrich incoming Thrift records and store the results
   * in the appropriate sinks. If doing so causes the number of events
   * stored in a sink to become sufficiently large, all sinks are flushed
   * and we return `true`, signalling that it is time to checkpoint
   * @param binaryData Thrift raw event
   * @return Whether to checkpoint
   */
  def enrichAndStoreEvents(binaryData: List[Array[Byte]]): Boolean = {
    val enrichedEvents = binaryData.flatMap(enrichEvents)
    val successes = enrichedEvents.collect { case Valid(s) => s }
    val sizeUnadjustedFailures = enrichedEvents.collect { case Invalid(s) => s }
    val failures = sizeUnadjustedFailures.map(_.toList).flatten.map {
      case (value, key) =>
        MaxRecordSize.flatMap(s => if (Source.getSizeBr(value) >= s) s.some else none) match {
          case None => value -> key
          case Some(s) => Source.adjustOversizedFailureJson(value, s, processor) -> key
        }
    }

    val (tooBigSuccesses, smallEnoughSuccesses) =
      successes.partition { s =>
        isTooLarge(s._1)
      }

    val sizeBasedFailures = for {
      (value, key, _) <- tooBigSuccesses
      m <- MaxRecordSize
    } yield Source.oversizedSuccessToFailure(value, m, processor) -> key

    val anonymizedSuccesses = smallEnoughSuccesses.map {
      case (event, partition, _) => (event, partition)
    }
    val piiSuccesses = smallEnoughSuccesses.flatMap {
      case (_, partition, pii) => pii.map((_, partition))
    }

    val successesTriggeredFlush = threadLocalGoodSink.get.storeEnrichedEvents(anonymizedSuccesses)
    val piiTriggeredFlush =
      threadLocalPiiSink.map(_.get.storeEnrichedEvents(piiSuccesses)).getOrElse(false)
    val allFailures = (failures ++ sizeBasedFailures)
      .map { case (br, k) => Source.brToString(br) -> k }
    val failuresTriggeredFlush =
      threadLocalBadSink.get.storeEnrichedEvents(allFailures)

    if (successesTriggeredFlush == true || failuresTriggeredFlush == true || piiTriggeredFlush == true) {
      // Block until the records have been sent to Kinesis
      threadLocalGoodSink.get.flush
      threadLocalPiiSink.map(_.get.flush)
      threadLocalBadSink.get.flush
      true
    } else {
      false
    }
  }

  private val isTooLarge: String => Boolean = evt =>
    MaxRecordSize.map(Source.getSize(evt) >= _).getOrElse(false)
}
