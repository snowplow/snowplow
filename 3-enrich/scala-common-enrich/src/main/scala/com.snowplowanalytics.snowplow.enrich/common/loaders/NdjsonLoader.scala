/*
 * Copyright (c) 2015-2020 Snowplow Analytics Ltd. All rights reserved.
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

import com.snowplowanalytics.snowplow.badrows._

import utils.JsonUtils

final case class NdjsonLoader(adapter: String) extends Loader[String] {

  private val CollectorName = "ndjson"
  private val CollectorEncoding = "UTF-8"

  /**
   * Converts the source string into a CanonicalInput.
   * @param line A line of data to convert
   * @return a CanonicalInput object, Option-boxed, or None if no input was extractable.
   */
  override def toCollectorPayload(line: String, processor: Processor): ValidatedNel[BadRow.CPFormatViolation, Option[CollectorPayload]] = {
    val collectorPayload =
      if (line.replaceAll("\r?\n", "").isEmpty)
        None.asRight
      else if (line.split("\r?\n").length > 1)
        FailureDetails.CPFormatViolationMessage
          .Fallback(s"expected a single line, found ${line.split("\r?\n").length}")
          .asLeft
      else
        for {
          _ <- JsonUtils
            .extractJson(line)
            .leftMap(FailureDetails.CPFormatViolationMessage.Fallback.apply)
          api <- CollectorPayload.parseApi(adapter)
          source = CollectorPayload.Source(CollectorName, CollectorEncoding, None)
          context = CollectorPayload.Context(None, None, None, None, Nil, None)
          payload = CollectorPayload(api, Nil, None, Some(line), source, context)
        } yield payload.some

    collectorPayload
      .leftMap(
        message =>
          BadRow.CPFormatViolation(
            processor,
            Failure.CPFormatViolation(Instant.now(), CollectorName, message),
            Payload.RawPayload(line)
          )
      )
      .toValidatedNel
  }
}
