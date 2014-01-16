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

// Java
import java.util.List
import java.nio.ByteBuffer

// Scala
import scala.io
import scala.util.control.Breaks._
import scala.collection.JavaConversions._

// Thrift
import org.apache.thrift.TDeserializer

// Apache commons
import org.apache.commons.codec.binary.Base64

// Decode Base64 Thrift objects from stdin.
class StdinSource(config: KinesisEnrichConfig)
    extends AbstractSource(config) {
  def run = {
    for (ln <- io.Source.stdin.getLines) {
      val bytes = Base64.decodeBase64(ln)
      enrichEvent(bytes)
    }
  }
}
