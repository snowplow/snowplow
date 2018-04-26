package com.snowplowanalytics
package snowplow.enrich
package beam

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets.UTF_8

import scalaz._

import common.outputs.{EnrichedEvent, BadRow}
import iglu.client.validation.ProcessingMessageMethods._

object utils {

  def tabSeparatedEnrichedEvent(enrichedEvent: EnrichedEvent): String =
    enrichedEvent.getClass.getDeclaredFields
    .map { field =>
      field.setAccessible(true)
      Option(field.get(enrichedEvent)).getOrElse("")
    }.mkString("\t")

  def resizeEnrichedEvent(enrichedEvent: String, bytesSize: Int, maxBytesSize: Int): BadRow =
    BadRow(enrichedEvent.take(maxBytesSize / (4 * 10)), NonEmptyList(
      s"Size of enriched event ($bytesSize) is greater than allowed maximum ($maxBytesSize)"))

  def resizeBadRow(badRow: BadRow, maxBytesSize: Int): BadRow = {
    val size = getStringSize(badRow.line)
    if (size >= maxBytesSize) {
      val msg = s"Size of bad row ($size) is greater than allowed maximum size ($maxBytesSize)"
      badRow.copy(
        line = badRow.line.take(maxBytesSize / (4 * 10)),
        errors = msg.toProcessingMessage <:: badRow.errors
      )
    } else badRow
  }

  def getStringSize(string: String): Int =
    ByteBuffer.wrap(string.getBytes(UTF_8)).capacity

}
