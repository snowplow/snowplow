/*
 * Copyright (c) 2012-2014 Snowplow Analytics Ltd. All rights reserved.
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

// Scalaz
import scalaz._
import Scalaz._

/**
 * Loader for TSVs
 */
case class TsvLoader(adapter: String) extends Loader[String] {

  /**
   * Converts the source TSV into a ValidatedMaybeCollectorPayload.
   *
   * @param line A TSV
   * @return either a set of validation errors or an Option-boxed
   *         CanonicalInput object, wrapped in a Scalaz ValidationNel.
   */
  def toCollectorPayload(line: String): ValidatedMaybeCollectorPayload =

    // Throw away the first two lines of Cloudfront web distribution access logs
    if (line.startsWith("#Version:") || line.startsWith("#Fields:")) {
      None.success
    } else {
      CollectorApi.parse(adapter).map(
        CollectorPayload(
          Nil,
          "tsv",
          "UTF-8",
          None,
          None,
          None,
          None,
          None,
          Nil,
          None,
          _,
          None,
          Some(line)
        ).some
      ).toValidationNel
    }
}
