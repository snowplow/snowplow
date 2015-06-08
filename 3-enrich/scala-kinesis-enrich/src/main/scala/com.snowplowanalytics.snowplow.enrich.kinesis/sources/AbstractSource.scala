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
package kinesis
package sources

// Java
import java.nio.ByteBuffer

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
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

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
import common.adapters.AdapterRegistry

import common.ValidatedMaybeCollectorPayload
import common.EtlPipeline
import common.utils.JsonUtils

/**
 * Abstract base for the different sources
 * we support.
 */
abstract class AbstractSource(config: KinesisEnrichConfig, igluResolver: Resolver,
                              enrichmentRegistry: EnrichmentRegistry) {
  
  val MaxRecordSize = 1000000L

  /**
   * Never-ending processing loop over source stream.
   */
  def run

  // Initialize a kinesis provider to use with a Kinesis source or sink.
  protected val kinesisProvider = config.credentialsProvider

  // Initialize the sink to output enriched events to.
  protected val sink: Option[ISink] = config.sink match {
    case Sink.Kinesis => new KinesisSink(kinesisProvider, config, InputType.Good).some
    case Sink.Stdouterr => new StdouterrSink(InputType.Good).some
    case Sink.Test => None
  }

  protected val badSink: Option[ISink] = config.sink match {
    case Sink.Kinesis => new KinesisSink(kinesisProvider, config, InputType.Bad).some
    case Sink.Stdouterr => new StdouterrSink(InputType.Bad).some
    case Sink.Test => None
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
      enrichmentRegistry, s"kinesis-${generated.Settings.version}", System.currentTimeMillis.toString, canonicalInput)
    processedEvents.map(validatedMaybeEvent => {
      validatedMaybeEvent match {
        case Success(co) => (tabSeparateEnrichedEvent(co) -> co.user_ipaddress).success
        case Failure(errors) => {
          val line = new String(Base64.encodeBase64(binaryData))
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
        val size = getSize(value)
        compact(render(try {
          val originalJson = parse(value)
          val errors = originalJson \ "errors"
          ("size" -> size) ~ ("errors" -> errors)
        } catch {
          case NonFatal(e) => ("size" -> size) ~
            ("errors" -> List("Unable to extract errors field from original oversized bad row JSON"))
        })) -> key
      }
    }

    val (tooBigSuccesses, smallEnoughSuccesses) = successes partition { s => isTooLarge(s._1) }
    val sizeBasedFailures = tooBigSuccesses map {
      case (value, key) => {
        val size = getSize(value)
        val errorJson =
          ("size" -> size) ~
          ("errors" -> List(s"Enriched event size of $size bytes is greater than allowed maximum of $MaxRecordSize"))
        compact(render(errorJson)) -> key
      }
    }

    val successesTriggeredFlush = sink.map(_.storeEnrichedEvents(smallEnoughSuccesses))
    val failuresTriggeredFlush = badSink.map(_.storeEnrichedEvents(failures ++ sizeBasedFailures))
    if (successesTriggeredFlush == Some(true) || failuresTriggeredFlush == Some(true)) {

      // Block until the records have been sent to Kinesis
      sink.foreach(_.flush)
      badSink.foreach(_.flush)
      true
    } else {
      false
    }

  }

  /**
   * The size of a string in bytes
   *
   * @param evt
   * @return size
   */
  private def getSize(evt: String): Long = ByteBuffer.wrap(evt.getBytes).capacity

  /**
   * Whether a record is too large to send to Kinesis
   *
   * @param evt
   * @return boolean size decision
   */
  private def isTooLarge(evt: String): Boolean = getSize(evt) >= MaxRecordSize
}
