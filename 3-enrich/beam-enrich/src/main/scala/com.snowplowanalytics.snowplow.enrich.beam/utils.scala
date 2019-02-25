/*
 * Copyright (c) 2012-2019 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at
 * http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and
 * limitations there under.
 */
package com.snowplowanalytics
package snowplow.enrich
package beam

import java.io.File
import java.net.URI
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path, Paths}
import java.util.UUID

import scala.util.Try

import org.joda.time.{DateTime, DateTimeZone}
import org.joda.time.format.DateTimeFormat
import org.json4s.{JObject, JValue}
import org.json4s.JsonDSL._
import org.json4s.jackson.Serialization.write
import scalaz._

import common.enrichments.EnrichmentRegistry
import common.outputs.{EnrichedEvent, BadRow}
import iglu.client.validation.ProcessingMessageMethods._
import singleton._

object utils {

  /** Format an [[EnrichedEvent]] as a TSV. */
  def tabSeparatedEnrichedEvent(enrichedEvent: EnrichedEvent): String =
    enrichedEvent.getClass.getDeclaredFields
    .filterNot(_.getName.equals("pii"))
    .map { field =>
      field.setAccessible(true)
      Option(field.get(enrichedEvent)).getOrElse("")
    }.mkString("\t")

  private val formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS")
  private implicit val formats = org.json4s.DefaultFormats
  /** Creates a PII event from the pii field of an existing event. */
  def getPiiEvent(event: EnrichedEvent): Option[EnrichedEvent] =
    Option(event.pii)
      .filter(_.nonEmpty)
      .map { piiStr =>
        val ee = new EnrichedEvent
        ee.unstruct_event = event.pii
        ee.app_id = event.app_id
        ee.platform = "srv"
        ee.etl_tstamp = event.etl_tstamp
        ee.collector_tstamp = event.collector_tstamp
        ee.event = "pii_transformation"
        ee.event_id = UUID.randomUUID().toString
        ee.derived_tstamp = formatter.print(DateTime.now(DateTimeZone.UTC))
        ee.true_tstamp = ee.derived_tstamp
        ee.event_vendor = "com.snowplowanalytics.snowplow"
        ee.event_format = "jsonschema"
        ee.event_name = "pii_transformation"
        ee.event_version = "1-0-0"
        ee.v_etl = s"beam-enrich-${generated.BuildInfo.version}-common-${generated.BuildInfo.sceVersion}"
        ee.contexts = write(getContextParentEvent(ee.event_id))
        ee
      }

  private def getContextParentEvent(eventId: String): JValue =
    ("schema" -> "iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-0") ~
      ("data" -> List(
        ("schema" -> "iglu:com.snowplowanalytics.snowplow/parent_event/jsonschema/1-0-0") ~
          ("data" -> ("parentEventId" -> eventId))))

  /** Determine if we have to emit pii transformation events. */
  def emitPii(registry: EnrichmentRegistry): Boolean =
    registry.getPiiPseudonymizerEnrichment.map(_.emitIdentificationEvent).getOrElse(false)

  // We want to take one-tenth of the payload characters and one character can take up to 4 bytes
  private val ReductionFactor = 4 * 10

  /**
   * Truncate an oversized formatted enriched event into a [[BadRow]].
   * @param enrichedEvent TSV-formatted oversized enriched event
   * @param bytesSize size in bytes of the formatted enriched event
   * @param maxBytesSize maximum size in bytes a record can take
   * @return a [[BadRow]] containing a the truncated enriched event (10 times less than the max size)
   */
  def resizeEnrichedEvent(enrichedEvent: String, bytesSize: Int, maxBytesSize: Int): BadRow =
    BadRow(enrichedEvent.take(maxBytesSize / ReductionFactor), NonEmptyList(
      s"Size of enriched event ($bytesSize) is greater than allowed maximum ($maxBytesSize)"))

  /**
   * Resize a [[BadRow]] if it exceeds the maximum allowed size.
   * @param badRow the original [[BadRow]] which can be oversized
   * @param maxBytesSize maximum size in bytes a record can take
   * @return a [[BadRow]] where the line is 10 times less than the max size
   */
  def resizeBadRow(badRow: BadRow, maxBytesSize: Int): BadRow = {
    val size = getStringSize(badRow.line)
    if (size >= maxBytesSize) {
      val msg = s"Size of bad row ($size) is greater than allowed maximum size ($maxBytesSize)"
      badRow.copy(
        line = badRow.line.take(maxBytesSize / ReductionFactor),
        errors = msg.toProcessingMessage <:: badRow.errors
      )
    } else badRow
  }

  /** Get the size of a string in bytes. */
  def getStringSize(string: String): Int = string.getBytes(UTF_8).size

  /** Measure the time spent in a block of code in milliseconds. */
  def timeMs[A](call: => A): (A, Long) = {
    val t0 = System.currentTimeMillis()
    val result = call
    val t1 = System.currentTimeMillis()
    (result, t1 - t0)
  }

  /**
   * Create a symbolic link.
   * @param file to create the sym link for
   * @param symLink path to the symbolic link to be created
   * @return either the path of the created sym link or the error
   */
  def createSymLink(file: File, symLink: String): Either[String, Path] = {
    val symLinkPath = Paths.get(symLink)
    if (!Files.exists(symLinkPath)) {
      Try(Files.createSymbolicLink(symLinkPath, file.toPath)) match {
        case scala.util.Success(p) => Right(p)
        case scala.util.Failure(t) => Left(s"Symlink can't be created: ${t.getMessage}")
      }
    } else Left(s"A file at path $symLinkPath already exists")
  }

  /**
   * Interrogates the enrichment registry to know which files need caching.
   * @param resolverJson the json configuration of the resolver
   * @param registryJson the json configuration of the enrichment registry
   * @return a list of tuples with the uri of the file that needs caching and the sym link that
   * needs to be created
   */
  def getFilesToCache(resolverJson: JValue, registryJson: JObject): List[(URI, String)] = {
    implicit val resolver = ResolverSingleton.get(resolverJson)
    val registry = EnrichmentRegistrySingleton.get(registryJson)
    registry.filesToCache
  }

  /**
   * Set up dynamic counter metrics from an [[EnrichedEvent]].
   * @param enrichedEvent to extract the metrics from
   * @return the name of the counter metrics that needs to be incremented.
   */
  def getEnrichedEventMetrics(enrichedEvent: EnrichedEvent): List[String] =
    List(
      Option(enrichedEvent.event_vendor).map(v => ("vendor", v)),
      Option(enrichedEvent.v_tracker).map(t => ("tracker", t))
    ).flatten
      .map { case (n, v) => n + "_" + v.replaceAll("[.-]", "_") }

}
