/*
 * Copyright (c) 2012-2013 SnowPlow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.hadoop.etl
package enrichments

// Scalaz
import scalaz._
import Scalaz._

/**
 * Type-based enrichments - not
 * specific to any one field.
 */
object TypedEnrichments {

  /**
   * Extract a Scala Int from
   * a String, or error.
   *
   * @param str The String
   *        which we hope is an
   *        Int
   * @param field The name of the
   *        field we are trying to
   *        process. To use in our
   *        error message
   * @return a Scalaz Validation,
   *         being either a
   *         Failure String or
   *         a Success Int
   */
  def extractInt(str: String, field: String): Validation[String, Int] = try {
      str.toInt.success
    } catch {
      case nfe: NumberFormatException =>
        "Field [%s]: cannot convert [%s] to Int".format(field, str).fail
    }

  /**
   * Extract a Scala Byte from
   * a String, or error.
   *
   * @param str The String
   *        which we hope is an
   *        Byte
   * @param field The name of the
   *        field we are trying to
   *        process. To use in our
   *        error message
   * @return a Scalaz Validation,
   *         being either a
   *         Failure String or
   *         a Success Byte
   */
  def extractByte(str: String, field: String): Validation[String, Byte] = try {
      str.toByte.success
    } catch {
      case nfe: NumberFormatException =>
        "Field [%s]: cannot convert [%s] to Byte".format(field, str).fail
    }
}