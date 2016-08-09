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
package com.snowplowanalytics.snowplow.storage.kinesis.elasticsearch

// Scalaz
import scalaz._
import Scalaz._

// Common Enrich
import com.snowplowanalytics.snowplow.enrich.common.outputs.BadRow

object FailureUtils {

  /**
   * Due to serialization issues we use List instead of NonEmptyList
   * so we need this method to convert the errors back to a NonEmptyList
   *
   * @param line
   * @param errors
   * @return Compact bad row JSON string
   */
  def getBadRow(line: String, errors: List[String]): String =
    BadRow(line, NonEmptyList(errors.head, errors.tail: _*)).toCompactJson
}
