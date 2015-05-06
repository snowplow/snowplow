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
package com.snowplowanalytics.snowplow
package collectors
package scalastream
package sinks

// Java
import java.nio.ByteBuffer

// Thrift
import org.apache.thrift.TSerializer

// Snowplow
import CollectorPayload.thrift.model1.CollectorPayload

// Define an interface for all sinks to use to store events.
trait AbstractSink {
  def storeRawEvent(event: CollectorPayload, key: String): Array[Byte]

  // Serialize Thrift CollectorPayload objects
  private val thriftSerializer = new ThreadLocal[TSerializer] {
    override def initialValue = new TSerializer()
  }

  def serializeEvent(event: CollectorPayload): Array[Byte] = {
    val serializer = thriftSerializer.get()
    serializer.serialize(event)
  }
}
