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
package com.snowplowanalytics
package snowplow
package enrich
package hadoop
package outputs

// Scalaz
import scalaz._
import Scalaz._

// json4s
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

// Iglu Scala Client
import iglu.client.ProcessingMessageNel

/**
 * Models our report on a bad row. Consists of:
 * 1. Our original input line (which was meant
 *    to be a Snowplow enriched event)
 * 2. A non-empty list of our Validation errors
 */
case class BadRow(
  val line: String,
  val errors: ProcessingMessageNel
  ) {

  /**
   * Converts a TypeHierarchy into a JSON containing
   * each element.
   *
   * @return the TypeHierarchy as a json4s JValue
   */
  // TODO: fix issue where the errors are being
  // stringified before being added
  def toJValue: JValue =
    ("line"     -> line) ~
    ("errors"   -> errors.map(e => fromJsonNode(e.asJson)).toList)

  /**
   * Converts our BadRow into a single JSON encapsulating
   * both the input line and errors.
   *
   * @return this BadRow as a compact stringified JSON
   */
  def toCompactJson: String =
    compact(this.toJValue)
}
