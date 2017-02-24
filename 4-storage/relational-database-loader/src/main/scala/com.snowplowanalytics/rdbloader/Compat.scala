/*
 * Copyright (c) 2017 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.rdbloader

// Scala
import scala.language.implicitConversions

// Circe
import io.circe.Json

// Json4s
import org.json4s.JsonAST._

object Compat {
  implicit def jvalueToCirce(jValue: JValue): Json = jValue match {
    case JString(s)    => Json.fromString(s)
    case JObject(vals) => Json.fromFields(vals.map { case (k, v) => (k, jvalueToCirce(v))})
    case JInt(i)       => Json.fromBigInt(i)
    case JDouble(d)    => Json.fromDoubleOrNull(d)
    case JBool(b)      => Json.fromBoolean(b)
    case JArray(arr)   => Json.arr(arr.map(jvalueToCirce): _*)
    case JNull         => Json.Null
    case JDecimal(d)   => Json.fromBigDecimal(d)
    case JNothing      => Json.Null
  }
}
