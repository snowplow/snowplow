/*
 * Copyright (c) 2014-2019 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
 */
package com.snowplowanalytics.snowplow.enrich.common.outputs

import com.snowplowanalytics.iglu.client.ProcessingMessageNel
import com.snowplowanalytics.iglu.client.validation.ProcessingMessageMethods._
import io.circe._
import io.circe.jackson._
import io.circe.syntax._
import org.joda.time.{DateTime, DateTimeZone}
import org.joda.time.format.DateTimeFormat
import scalaz._
import Scalaz._

/** Alternate BadRow constructors */
object BadRow {

  /**
   * Constructor using Strings instead of ProcessingMessages
   * @param line
   * @param errors
   * @return a BadRow
   */
  def apply(line: String, errors: NonEmptyList[String]): BadRow =
    BadRow(line, errors.map(_.toProcessingMessage), System.currentTimeMillis())

  /**
   * For rows which are so too long to send to Kinesis and so cannot contain their own original line
   * @param line
   * @param errors
   * @param tstamp
   * @return a BadRow
   */
  def oversizedRow(
    size: Long,
    errors: NonEmptyList[String],
    tstamp: Long = System.currentTimeMillis()
  ): String =
    Json
      .obj(
        "size" := Json.fromLong(size),
        "errors" := Json.arr(
          errors.toList.map(e => jacksonToCirce(e.toProcessingMessage.asJson)): _*),
        "failure_tstamp" := Json.fromLong(tstamp)
      )
      .noSpaces
}

/**
 * Models our report on a bad row. Consists of:
 * 1. Our original input line (which was meant to be a Snowplow enriched event)
 * 2. A non-empty list of our Validation errors
 * 3. A timestamp
 */
final case class BadRow(
  val line: String,
  val errors: ProcessingMessageNel,
  val tstamp: Long = System.currentTimeMillis()
) {

  // An ISO valid timestamp formatter
  private val TstampFormat =
    DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").withZone(DateTimeZone.UTC)

  /**
   * Converts a TypeHierarchy into a JSON containing each element.
   * @return the TypeHierarchy as a Json
   */
  def toJson: Json =
    Json.obj(
      "line" := Json.fromString(line),
      "errors" := Json.arr(errors.toList.map(e => jacksonToCirce(e.asJson)): _*),
      "failure_tstamp" := getTimestamp(tstamp)
    )

  /**
   * Converts our BadRow into a single JSON encapsulating both the input line and errors.
   * @return this BadRow as a compact stringified JSON
   */
  def toCompactJson: String = toJson.noSpaces

  /**
   * Returns an ISO valid timestamp
   * @param tstamp The Timestamp to convert
   * @return the formatted Timestamp
   */
  def getTimestamp(tstamp: Long): String = {
    val dt = new DateTime(tstamp)
    TstampFormat.print(dt)
  }
}
