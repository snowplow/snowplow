/*
 * Copyright (c) 2012-2019 Snowplow Analytics Ltd. All rights reserved.
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
import com.snowplowanalytics.snowplow.badrows._

/** Loader for TSVs */
final case class TsvLoader(adapter: String) extends Loader[String] {
  private val CollectorName = "tsv"
  private val CollectorEncoding = "UTF-8"

  /**
   * Converts the source TSV into a ValidatedMaybeCollectorPayload.
   * @param line A TSV
   * @return either a set of validation errors or an Option-boxed CanonicalInput object, wrapped in
   * a ValidatedNel.
   */
  override def toCollectorPayload(
    line: String,
    processor: Processor
  ): ValidatedNel[BadRow.CPFormatViolation, Option[CollectorPayload]] =
    // Throw away the first two lines of Cloudfront web distribution access logs
    if (line.startsWith("#Version:") || line.startsWith("#Fields:"))
      None.valid
    else
      CollectorPayload
        .parseApi(adapter)
        .map { api =>
          val source = CollectorPayload.Source(CollectorName, CollectorEncoding, None)
          val context = CollectorPayload.Context(None, None, None, None, Nil, None)
          CollectorPayload(api, Nil, None, Some(line), source, context).some
        }
        .leftMap(
          f =>
            BadRow.CPFormatViolation(
              processor,
              Failure.CPFormatViolation(Instant.now(), CollectorName, f),
              Payload.RawPayload(line)
            )
        )
        .toValidatedNel
}
