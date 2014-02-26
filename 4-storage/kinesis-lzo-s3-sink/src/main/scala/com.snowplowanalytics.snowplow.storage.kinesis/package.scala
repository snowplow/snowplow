 /*
 * Copyright (c) 2015 Snowplow Analytics Ltd.
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

package com.snowplowanalytics.snowplow.storage.kinesis

// Scalaz
import scalaz._
import Scalaz._

package object s3 {

  /**
   * Tuple containing:
   *  - the original Kinesis record, base 64 encoded
   *  - a validated SnowplowRawEvent created from it
   */
  type ValidatedRecord = (String, Validation[List[String], Array[Byte]])

  /**
   * Currently the same as ValidatedRecord, but could change in the future
   */
  type EmitterInput = (String, Validation[List[String], Array[Byte]])
}
