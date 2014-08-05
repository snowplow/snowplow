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
package adapters
package registry

// Scalaz
import scalaz._
import Scalaz._

// This project
import loaders.CollectorPayload

trait Adapter {
  def toRawEvents(payload: CollectorPayload): ValidatedRawEvents

  // TODO: add a helper method for "map is empty"

  /**
   *
   *
   *
  def failIfEmpty(parameters: Map[String, String]): XXX */

  /**
   * Converts a NonEmptyList of name:value
   * pairs into a Map.
   *
   * @param parameters A NonEmptyList of name:value pairs
   * @return the name:value pairs in Map form
   */
  def toMap(parameters: NameValueNel): Map[String, String] =
    parameters.map(p => (p.getName -> p.getValue)).toList.toMap
}
