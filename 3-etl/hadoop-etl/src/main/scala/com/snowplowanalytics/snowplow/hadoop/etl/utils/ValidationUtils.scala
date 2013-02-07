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
package utils

// Scalaz
import scalaz._
import Scalaz._

/**
 * General-purpose utils to help with Scalaz Validations.
 */
object ValidationUtils {

  // Placeholders for where the Success value doesn't matter.
  // This might sound like heresy but is actually quite useful
  // when you're slowly updating large (>22 field) POSOs.
  val unitSuccess = ().success[String]
  val unitSuccessNel = ().successNel[String]

  /**
   * A helper to strip out a Success from a ValidationNEL
   * and turn it into a Unit. Useful if you want to sum
   * Validations for the Failures rather than the Successes.
   * Because Semigroup[Any] doesn't exist (and shouldn't).
   *
   * @param v The ValidationNEL to convert
   * @return the same ValidationNEL but with the Success converted
   *         into a Unit
   */
  def toUnitSuccess(v: ValidationNEL[String, _]): ValidationNEL[String, Unit] =
    v.flatMap(s => unitSuccessNel)
}