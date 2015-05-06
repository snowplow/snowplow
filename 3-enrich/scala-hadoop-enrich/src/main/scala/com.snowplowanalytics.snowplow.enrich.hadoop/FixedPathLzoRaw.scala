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
package com.snowplowanalytics.snowplow.enrich.hadoop

// Cascading
import cascading.scheme.Scheme

// Scalding
import com.twitter.scalding._
import com.twitter.scalding.commons.source._

// Elephant Bird
import com.twitter.elephantbird.cascading2.scheme.LzoByteArrayScheme

/**
 * Scalding source for LZO-compressed binary arrays
 */
case class FixedPathLzoRaw(path: String*) extends FixedPathSource(path: _*) with LzoRaw

/**
 * Equivalent to the LzoThrift trait, by with Array[Byte] in place of TBase
 */
trait LzoRaw extends LocalTapSource with SingleMappable[Array[Byte]] with TypedSink[Array[Byte]] {
  override def setter[U <: Array[Byte]] = TupleSetter.asSubSetter[Array[Byte], U](TupleSetter.singleSetter[Array[Byte]])
  override def hdfsScheme = HadoopSchemeInstance((new LzoByteArrayScheme()).asInstanceOf[Scheme[_, _, _, _, _]])
}
