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

// Cascading
import cascading.pipe.Pipe
import cascading.tuple.{TupleEntry, Fields}

// Scalding
import com.twitter.scalding.{Dsl, FixedPathSource, TextLineScheme}

// Jerkson
import com.codahale.jerkson.Json

/**
 * This Source writes out the TupleEntry as a simple JSON object, using the field names
 * as keys and the string representation of the values.
 * Only useful for writing, on read it is identical to TextLineScheme.
 *
 * TODO: this can be removed when Scalding 0.8.3 is released.
 */
case class Json2Line(p : String) extends FixedPathSource(p) with TextLineScheme {
  import Dsl._

  /**
   * Updated by SnowPlow so that a Scala List of errors
   * is turned into valid JSON.
   */
  override def transformForWrite(pipe : Pipe) = pipe.mapTo(Fields.ALL -> 'json) {
    t: TupleEntry => Json.generate(toMap(t))
  }
}