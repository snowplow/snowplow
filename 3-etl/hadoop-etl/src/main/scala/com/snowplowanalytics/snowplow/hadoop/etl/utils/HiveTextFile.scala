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

/**
 * Helpers to generate the
 * TEXTFILE format used by
 * Apache Hive.
 *
 * See:
 * http://www.nearinfinity.com/blogs/stephen_mouring_jr/2013/01/04/writing-hive-tables-from-mapreduce.html
 */
object HiveTextFile {

  /**
   * Hive uses:
   * * \001 (SOH) to delimit fields
   * * \002 (STX_) to delimit array values
   *   within fields
   */
  private object Separators {
    val Field = 0x001.toChar.toString // SOH
    val ArrayValue = 0x002.toChar.toString // STX
  }

  /**
   * Helper to convert any Scala
   * Iterable to a Hive-formatted
   * ARRAY.
   *
   * @param i the Iterable to
   * turn into a Hive ARRAY
   * @return the String
   * representation of this
   * Iterable, with the Hive ARRAY
   * delimiter between each element
   */
  private def toHiveArray[A](i: Iterable[A]) =
    i.mkString(Separators.ArrayValue)

  /**
   * Helper to convert a Boolean
   * value into a Byte (1 or 0).
   *
   * This is used when generating
   * "non-Hive format" Hive files.
   *
   * @param b The Boolean to turn
   *        into a Byte
   * @return the Byte value of b 
   */
  private def toByte(b: Boolean): Byte =
    if (b) 1.toByte else 0.toByte
}