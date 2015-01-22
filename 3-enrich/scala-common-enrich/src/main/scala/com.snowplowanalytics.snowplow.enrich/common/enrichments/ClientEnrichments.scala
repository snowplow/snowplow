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
package enrichments

// Java
import java.lang.{Integer => JInteger}

// Scalaz
import scalaz._
import Scalaz._

/**
 * Contains enrichments related to the
 * client - where the client is the
 * software which is using the SnowPlow
 * tracker.
 *
 * Enrichments relate to browser resolution
 */
object ClientEnrichments {
  
  /**
   * The Tracker Protocol's pattern
   * for a screen resolution - for
   * details see:
   *
   * https://github.com/snowplow/snowplow/wiki/snowplow-tracker-protocol#wiki-browserandos
   */
  private val ResRegex = """(\d+)x(\d+)""".r

  /**
   * Extracts view dimensions (e.g. screen resolution,
   * browser/app viewport) stored as per the Tracker
   * Protocol:
   *
   * https://github.com/snowplow/snowplow/wiki/snowplow-tracker-protocol#wiki-browserandos
   *
   * @param field The name of the field
   *        holding the screen dimensions
   * @param res The packed string
   *        holding the screen dimensions
   * @return the ResolutionTuple or an
   *         error message, boxed in a
   *         Scalaz Validation
   */
  val extractViewDimensions: (String, String) => Validation[String, ViewDimensionsTuple] = (field, res) =>
    res match {
      case ResRegex(width, height) =>
        try {
          (width.toInt: JInteger, height.toInt: JInteger).success
        } catch {
          case _ => "Field [%s]: view dimensions [%s] exceed Integer's max range".format(field, res).fail
        }
      case _ => "Field [%s]: [%s] does not contain valid view dimensions".format(field, res).fail
    }

}
