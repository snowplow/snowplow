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
package com.snowplowanalytics.snowplow.collectors
package scalastream
package sinks

// Java
import java.nio.ByteBuffer

// Apache Commons
import org.apache.commons.codec.binary.Base64

// Thrift
import org.apache.thrift.TSerializer

// Config
import com.typesafe.config.Config

// Snowplow
import scalastream._
import thrift.SnowplowRawEvent

class StdoutSink extends AbstractSink {
  // Print a Base64-encoded event.
  def storeRawEvent(event: SnowplowRawEvent, key: String) = {
    println(Base64.encodeBase64String(serializeEvent(event)))
    null
  }
}
