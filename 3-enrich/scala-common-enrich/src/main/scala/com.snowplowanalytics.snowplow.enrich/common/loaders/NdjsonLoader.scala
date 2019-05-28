/*
 * Copyright (c) 2015-2019 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.enrich.common
package loaders

import java.time.Instant

import cats.data.ValidatedNel
import cats.syntax.either._
import cats.syntax.option._
import cats.syntax.validated._

import outputs._
import utils.JsonUtils

final case class NdjsonLoader(adapter: String) extends Loader[String] {

  private val CollectorName = "ndjson"
  private val CollectorEncoding = "UTF-8"

  /**
   * Converts the source string into a CanonicalInput.
   * @param line A line of data to convert
   * @return a CanonicalInput object, Option-boxed, or None if no input was extractable.
   */
  override def toCP(line: String): ValidatedNel[BadRow, Option[CollectorPayload]] =
    (if (line.replaceAll("\r?\n", "").isEmpty) {
       None.validNel
     } else if (line.split("\r?\n").size > 1) {
       val size = line.split("\r?\n").size
       FallbackCPFormatViolationMessage(s"expected a single line, found $size").invalidNel
     } else {
       (for {
         _ <- JsonUtils
           .extractJson(line)
           .leftMap(FallbackCPFormatViolationMessage.apply)
         collectorApi <- CollectorApi.parsePath(adapter)
         cp = CollectorPayload(
           Nil,
           CollectorName,
           CollectorEncoding,
           None,
           None,
           None,
           None,
           None,
           Nil,
           None,
           collectorApi,
           None,
           Some(line)
         ).some
       } yield cp).toValidatedNel
     }).leftMap(
      _.map(
        f =>
          BadRow(
            CPFormatViolation(Instant.now(), CollectorName, f),
            RawPayload(line),
            Processor.default
          )
      )
    )
}
