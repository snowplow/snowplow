/*
 * Copyright (c) 2012-2014 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.enrich.common
package inputs

// Snowplow.
import com.snowplowanalytics.snowplow.collectors.thrift.{
  SnowplowEvent,
  TrackerPayload => ThriftTrackerPayload,
  PayloadProtocol,
  PayloadFormat
}

// Scalaz
import scalaz._
import Scalaz._

// Apache Commons
import org.apache.commons.lang3.StringUtils

// Joda-Time
import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}

// Thrift.
import org.apache.thrift.TDeserializer

/**
 * Loader for Thrift SnowplowRawEvent objects.
 */
class ThriftLoader extends CollectorLoader {
  private val thriftDeserializer = new TDeserializer

  /**
   * Converts the source string into a MaybeCanonicalInput.
   *
   * @param line A line of data to convert
   * @return either a set of validation errors or an Option-boxed
   *         CanonicalInput object, wrapped in a Scalaz ValidatioNel.
   */
  def toCanonicalInput(line: String): ValidatedMaybeCanonicalInput = {
    var snowplowEvent = new SnowplowEvent
    try {
      // TODO: Currently, Thrift events are stored as a byte array
      // in Kinesis -- not as a Base64 string.
      // Is this fine, or should we change to storing in a Base64 string
      // so we're not passing a byte array as a string here?
      thriftDeserializer.deserialize(snowplowEvent, line.getBytes)

      // TODO: Check isIceRequest.

      /*
      TODO: Fill in and remove.
      timestamp:  DateTime, // Collector timestamp
      payload:    TrackerPayload, // See below for defn.
      source:     InputSource,    // See below for defn.
      encoding:   String, 
      ipAddress:  Option[String],
      userAgent:  Option[String],
      refererUri: Option[String],
      headers:    List[String],   // May be Nil so not a Nel
      userId:     Option[String])
      */
      Some(
        CanonicalInput(
          null,
          null,
          null,
          null,
          None,
          None,
          None,
          null,
          None
        )
      ).success
    } catch {
      // TODO: Check for deserialization errors.
      case _: Throwable =>
        "Line does not match Thrift object.".failNel[Option[CanonicalInput]]
    }
  }
}
