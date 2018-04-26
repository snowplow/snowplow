/*
 * Copyright (c) 2012-2018 Snowplow Analytics Ltd. All rights reserved.
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
