/*
 * Copyright (c) 2012-2020 Snowplow Analytics Ltd. All rights reserved.
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
package utils

import cats.data.Validated
import cats.syntax.either._

import io.circe._

object CirceUtils {

  /**
   * Returns a field of type A at the end of a JSON path
   * @tparam A Type of the field to extract
   * @param head The first field in the JSON path. Exists to ensure the path is nonempty
   * @param tail The rest of the fields in the JSON path
   * @return the list extracted from the JSON on success or an error String on failure
   */
  def extract[A: Decoder: Manifest](
    config: Json,
    head: String,
    tail: String*
  ): Validated[String, A] = {
    val path = head :: tail.toList
    path
      .foldLeft(config.hcursor: ACursor) { case (c, f) => c.downField(f) }
      .as[A]
      .toValidated
      .leftMap { e =>
        val pathStr = path.mkString(".")
        val clas = manifest[A]
        s"Could not extract $pathStr as $clas from supplied JSON due to ${e.getMessage}"
      }
  }
}
