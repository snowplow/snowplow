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
object HiveUtils {

  /**
   * Helper to convert any Scala Iterable to
   * a Hive-formatted ARRAY. Values in Hive
   * ARRAYs are separated by \002 (STX).
   *
   * @param i the Iterable to turn into a Hive ARRAY
   * @return a String containing all Iterable elements
   *         separated by STX
   */
  def toHiveArray[A](i: Iterable[A]): String =
    i.mkString("\2")

  // TODO: add support for Hive MAPs
}