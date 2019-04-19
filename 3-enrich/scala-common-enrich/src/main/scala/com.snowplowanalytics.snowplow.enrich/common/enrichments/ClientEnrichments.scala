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
package enrichments

import java.lang.{Integer => JInteger}

import cats.syntax.either._

/**
 * Contains enrichments related to the client - where the client is the software which is using the
 * Snowplow tracker. Enrichments relate to browser resolution.
 */
object ClientEnrichments {

  /**
   * The Tracker Protocol's pattern for a screen resolution - for details see:
   * https://github.com/snowplow/snowplow/wiki/snowplow-tracker-protocol#wiki-browserandos
   */
  private val ResRegex = """(\d+)x(\d+)""".r

  /**
   * Extracts view dimensions (e.g. screen resolution, browser/app viewport) stored as per the
   * Tracker Protocol:
   * https://github.com/snowplow/snowplow/wiki/snowplow-tracker-protocol#wiki-browserandos
   * @param field The name of the field holding the screen dimensions
   * @param res The packed string holding the screen dimensions
   * @return the ResolutionTuple or an error message, boxed in a Scalaz Validation
   */
  val extractViewDimensions: (String, String) => Either[String, (JInteger, JInteger)] =
    (field, res) =>
      res match {
        case ResRegex(width, height) =>
          Either
            .catchNonFatal((width.toInt: JInteger, height.toInt: JInteger))
            .leftMap(_ => s"Field [$field]: view dimensions [$res] exceed Integer's max range")
        case _ => s"Field [$field]: [$res] does not contain valid view dimensions".asLeft
      }

}
