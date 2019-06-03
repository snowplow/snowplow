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

// scalaz import have to be first, there is a weird conflict with json4s imports
import scalaz.{Sink => _, _}
import Scalaz._
import org.apache.commons.codec.binary.Base64
import org.json4s.{ThreadLocal => _, _}
import org.json4s.JsonDSL._
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.write
import org.json4s.jackson.JsonMethods._
import org.slf4j.LoggerFactory
import org.joda.time.{DateTime, DateTimeZone}
import org.joda.time.format.DateTimeFormat

import common.{EtlPipeline, ValidatedMaybeCollectorPayload}
import common.enrichments.EnrichmentRegistry
import common.adapters.AdapterRegistry
import common.loaders.ThriftLoader
import common.outputs.{BadRow, EnrichedEvent}

import iglu.client.Resolver
import scalatracker.Tracker
import sinks._

object Source {

  val PiiEventName = "pii_transformation"
  val PiiEventVendor = "com.snowplowanalytics.snowplow"
  val PiiEventFormat = "jsonschema"
  val PiiEventVersion = "1-0-0"
  val PiiEventPlatform = "srv"
  val ContextsSchema = "iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-0"
  val ParentEventSchema = "iglu:com.snowplowanalytics.snowplow/parent_event/jsonschema/1-0-0"

  /**
   * If a bad row JSON is too big, reduce it's size
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
      compact(render({ ("size" -> size): JValue } merge jsonWithoutLine))

    } catch {
      case NonFatal(e) =>
        BadRow.oversizedRow(
          size,
          NonEmptyList("Unable to extract errors field from original oversized bad row JSON")
        )
    }
  }

  /**
   * Convert a too-large successful event to a failure
   * @param value Event which passed enrichment but was too large
   * @param maximum Maximum allowable bytes
   * @return Bad row JSON
   */
  def oversizedSuccessToFailure(value: String, maximum: Long): String = {
    val size = Source.getSize(value)
    BadRow.oversizedRow(
      size,
      NonEmptyList(
        s"Enriched event size of $size bytes is greater than allowed maximum of $maximum"
      )
    )
  }

  /** The size of a string in bytes */
  def getSize(evt: String): Long = ByteBuffer.wrap(evt.getBytes(UTF_8)).capacity.toLong
}

/** Abstract base for the different sources we support. */
abstract class Source(
  igluResolver: Resolver,
  adapterRegistry: AdapterRegistry,
  enrichmentRegistry: EnrichmentRegistry,
  tracker: Option[Tracker],
  partitionKey: String
) {

  val MaxRecordSize: Option[Long]

  lazy val log = LoggerFactory.getLogger(getClass())

  /** Never-ending processing loop over source stream. */
  def run(): Unit

  implicit val resolver: Resolver = igluResolver
  private val formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS")

  def getPiiEvent(event: EnrichedEvent): Option[EnrichedEvent] =
    Option(event.pii)
      .filter(_.nonEmpty)
      .map { piiStr =>
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
        ee.contexts = getContextParentEvent(event.event_id)
        ee.v_etl =
          s"stream-enrich-${generated.BuildInfo.version}-common-${generated.BuildInfo.commonEnrichVersion}"
        ee
      }

  def getContextParentEvent(eventId: String): String = {
    implicit val json4sFormats = DefaultFormats

    write(
      ("schema" -> Source.ContextsSchema) ~ ("data" -> List(
        ("schema" -> Source.ParentEventSchema) ~ ("data" -> ("parentEventId" -> eventId))
      ))
    )
  }

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
   *
   * @param binaryData Thrift raw event
   * @return List containing failed, successful and, if present, pii events. Successful and failed, each specify a
   *         partition key.
   */
  def enrichEvents(
    binaryData: Array[Byte]
  ): List[Validation[(String, String), (String, String, Option[String])]] = {
    val canonicalInput: ValidatedMaybeCollectorPayload = ThriftLoader.toCollectorPayload(binaryData)
    val processedEvents: List[ValidationNel[String, EnrichedEvent]] = EtlPipeline.processEvents(
      adapterRegistry,
      enrichmentRegistry,
      s"stream-enrich-${generated.BuildInfo.version}",
      new DateTime(System.currentTimeMillis),
      canonicalInput
    )
    processedEvents.map(validatedMaybeEvent => {
      validatedMaybeEvent match {
        case Success(co) =>
          (
            tabSeparateEnrichedEvent(co),
            getProprertyValue(co, partitionKey),
            getPiiEvent(co).map(tabSeparateEnrichedEvent)
          ).success
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
   * @param binaryData Thrift raw event
   * @return Whether to checkpoint
   */
  def enrichAndStoreEvents(binaryData: List[Array[Byte]]): Boolean = {
    val enrichedEvents = binaryData.flatMap(enrichEvents(_))
    val successes = enrichedEvents collect { case Success(s) => s }
    val sizeUnadjustedFailures = enrichedEvents collect { case Failure(s) => s }
    val failures = sizeUnadjustedFailures map {
      case (value, key) =>
        if (!isTooLarge(value)) {
          value -> key
        } else {
          Source.adjustOversizedFailureJson(value) -> key
        }
    }

    val (tooBigSuccesses, smallEnoughSuccesses) = successes partition { s =>
      isTooLarge(s._1)
    }

    val sizeBasedFailures = for {
      (value, key, _) <- tooBigSuccesses
      m <- MaxRecordSize
    } yield Source.oversizedSuccessToFailure(value, m) -> key

    val anonymizedSuccesses = smallEnoughSuccesses.map {
      case (event, partition, _) => (event, partition)
    }
    val piiSuccesses = smallEnoughSuccesses.flatMap {
      case (_, partition, pii) => pii.map((_, partition))
    }

    val successesTriggeredFlush = threadLocalGoodSink.get.storeEnrichedEvents(anonymizedSuccesses)
    val piiTriggeredFlush =
      threadLocalPiiSink.map(_.get.storeEnrichedEvents(piiSuccesses)).getOrElse(false)
    val failuresTriggeredFlush =
      threadLocalBadSink.get.storeEnrichedEvents(failures ++ sizeBasedFailures)

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

  /**
   * Whether a record is too large to send to Kinesis
   * @param evt
   * @return boolean size decision
   */
  private def isTooLarge(evt: String): Boolean = MaxRecordSize match {
    case None => false
    case Some(m) => Source.getSize(evt) >= m
  }

}
