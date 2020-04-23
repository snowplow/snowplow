/*
 * Copyright (c) 2020-2020 Snowplow Analytics Ltd. All rights reserved.
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

import org.specs2.Specification

import io.circe.Json

class JsonUtilsSpec extends Specification {
  def is = s2"""
  toJson can deal with non-null String    $e1
  toJson can deal with null String        $e2
  """

  def e1 = {
    val key = "key"
    val value = "value"
    JsonUtils.toJson(key, value, Nil, Nil, None) must
      beEqualTo((key, Json.fromString(value)))
  }

  def e2 = {
    val key = "key"
    val value: String = null
    JsonUtils.toJson(key, value, Nil, Nil, None) must
      beEqualTo((key, Json.Null))
  }
}
