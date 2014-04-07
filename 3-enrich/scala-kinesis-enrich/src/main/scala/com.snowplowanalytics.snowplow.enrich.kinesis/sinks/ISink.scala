/*
 * Copyright (c) 2013-2014 Snowplow Analytics Ltd.
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
package com.snowplowanalytics.snowplow.enrich
package kinesis.sinks

/**
 * An interface for all sinks to use to store events.
 */
trait ISink {

  /**
   * Side-effecting function to store the CanonicalOutput
   * to the given output stream.
   *
   * CanonicalOutput takes the form of a tab-delimited
   * String until such time as https://github.com/snowplow/snowplow/issues/211
   * is implemented.
   */
  def storeCanonicalOutput(output: String, key: String)
}
