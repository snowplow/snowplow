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

// Snowplow
import thrift.SnowplowRawEvent

// Java
import java.nio.ByteBuffer

// Thrift
import org.apache.thrift.TSerializer

// Define an interface for all sinks to use to store events.
trait AbstractSink {
  def storeRawEvent(event: SnowplowRawEvent, key: String): Array[Byte]

  // Serialize Thrift SnowplowRawEvent objects,
  // and synchronize because TSerializer doesn't support multi-threaded
  // serialization.
  private val thriftSerializer = new TSerializer
  def serializeEvent(event: SnowplowRawEvent): Array[Byte] =
    this.synchronized {
      thriftSerializer.serialize(event)
    }
}
