/*
 * Copyright (c) 2013-2014 Snowplow Analytics Ltd. All rights reserved.
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

package com.snowplowanalytics.collectors
package scalastream
package backends

import scalastream._
import thrift.SnowplowEvent

import java.nio.ByteBuffer
import org.apache.thrift.TSerializer
import com.typesafe.config.Config
import org.apache.commons.codec.binary.Base64

/**
 * Stdout Backend for the Scala collector.
 */
object StdoutBackend {
  private val thriftSerializer = new TSerializer()
  def printEvent(event: SnowplowEvent) = {
    println(Base64.encodeBase64String(thriftSerializer.serialize(event)))
  }
}
