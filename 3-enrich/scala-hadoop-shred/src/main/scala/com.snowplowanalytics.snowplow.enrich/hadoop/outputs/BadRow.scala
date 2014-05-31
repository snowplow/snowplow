/*
 * Copyright (c) 2014 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.enrich
package hadoop
package outputs

// Scalaz
import scalaz._
import Scalaz._

/**
 * Models our report on a bad row. Consists of:
 * 1. Our original input line (which was meant
 *    to be a Snowplow enriched event)
 * 2. A non-empty list of our Validation errors
 */
case class BadRow(
  val line: String,
  val errors: ProcMsgNel
  ) {

  /**
   * Converts our BadRow into a single JSON encapsulating
   * both the input line and errors.
   *
   * @return this BadRow as a stringified JSON
   */
  // TODO: fix method name, not nice
  // TODO: clean up this implementation, it's hideous. It's here just to get tests passing
  def asJsonString: String = {
    val front = s"""{"line":"${line}","errors":["""
    val mid1  = errors.map(_.asJson.toString)
    val mid2  = mid1.toList.mkString("""",""")
    val end   = """]}"""
    front + mid2 + end
  }
}
