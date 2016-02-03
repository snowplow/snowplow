 /*
 * Copyright (c) 2014 Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache
 * License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at
 * http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied.
 *
 * See the Apache License Version 2.0 for the specific language
 * governing permissions and limitations there under.
 */
package com.snowplowanalytics.snowplow.storage.kinesis.elasticsearch.sinks

/**
 * Stdout/err sink
 */
class StdouterrSink extends ISink {

  /**
   * Writes a string to stdout or stderr
   *
   * @param output The string to write
   * @param key Unused parameter which exists to implement ISink
   * @param good Whether to write to stdout or stderr
   */
  def store(output: String, key: Option[String], good: Boolean) =
    if (good) {
      println(output) // To stdout
    } else {
      Console.err.println(output) // To stderr
    }
}
