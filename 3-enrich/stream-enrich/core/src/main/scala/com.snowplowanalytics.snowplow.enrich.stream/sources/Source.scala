/*
 * Copyright (c) 2013-2018 Snowplow Analytics Ltd.
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
import org.joda.time.DateTime
import org.json4s.{ThreadLocal => _, _}
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import org.slf4j.LoggerFactory

import common.{EtlPipeline, ValidatedMaybeCollectorPayload}
import common.enrichments.EnrichmentRegistry
import common.loaders.ThriftLoader
import common.outputs.{EnrichedEvent, BadRow}
import iglu.client.Resolver
import scalatracker.Tracker
import sinks._

object Source {

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
      compact(render({("size" -> size): JValue} merge jsonWithoutLine))
    } catch {
      case NonFatal(e) =>
        BadRow.oversizedRow(size, NonEmptyList("Unable to extract errors field from original oversized bad row JSON"))
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
    BadRow.oversizedRow(size, NonEmptyList(s"Enriched event size of $size bytes is greater than allowed maximum of $maximum"))
  }

  /** The size of a string in bytes */
  def getSize(evt: String): Long = ByteBuffer.wrap(evt.getBytes(UTF_8)).capacity.toLong
}

/** Abstract base for the different sources we support. */
abstract class Source(
  igluResolver: Resolver,
  enrichmentRegistry: EnrichmentRegistry,
  tracker: Option[Tracker],
  partitionKey: String
) {

  val MaxRecordSize: Option[Long]

  lazy val log = LoggerFactory.getLogger(getClass())

  /** Never-ending processing loop over source stream. */
  def run(): Unit

  implicit val resolver: Resolver = igluResolver

  val threadLocalGoodSink: ThreadLocal[Sink]
  val threadLocalBadSink: ThreadLocal[Sink]

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
      s"stream-enrich-${generated.BuildInfo.version}",
      new DateTime(System.currentTimeMillis),
      canonicalInput)
    processedEvents.map(validatedMaybeEvent => {
      validatedMaybeEvent match {
        case Success(co) =>
          (tabSeparateEnrichedEvent(co), getProprertyValue(co, partitionKey)).success
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
      case (value, key) => if (! isTooLarge(value)) {
        value -> key
      } else {
        Source.adjustOversizedFailureJson(value) -> key
      }
    }

    val (tooBigSuccesses, smallEnoughSuccesses) = successes partition { s => isTooLarge(s._1) }

    val sizeBasedFailures = for {
      (value, key) <- tooBigSuccesses
      m <- MaxRecordSize
    } yield Source.oversizedSuccessToFailure(value, m) -> key

    val successesTriggeredFlush = threadLocalGoodSink.get.storeEnrichedEvents(smallEnoughSuccesses)
    val failuresTriggeredFlush = threadLocalBadSink.get.storeEnrichedEvents(failures ++ sizeBasedFailures)
    if (successesTriggeredFlush == true || failuresTriggeredFlush == true) {
      // Block until the records have been sent to Kinesis
      threadLocalGoodSink.get.flush
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
