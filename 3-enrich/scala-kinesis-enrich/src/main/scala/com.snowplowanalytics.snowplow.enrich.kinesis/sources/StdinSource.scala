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

package com.snowplowanalytics.snowplow.enrich.kinesis
package sources

// Scala
import scala.io
import scala.collection.JavaConversions._

// Apache commons
import org.apache.commons.codec.binary.Base64

/**
 * Source to decode raw events (in base64)
 * from stdin.
 */
class StdinSource(config: KinesisEnrichConfig)
    extends AbstractSource(config) {

  /**
   * Never-ending processing loop over source stream.
   */
  def run = {
    for (ln <- io.Source.stdin.getLines) {
      val bytes = Base64.decodeBase64(ln)
      enrichEvent(bytes)
    }
  }
}
