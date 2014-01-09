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
  SnowplowRawEvent,
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

// Java conversions.
import scala.collection.JavaConversions._

/**
 * Loader for Thrift SnowplowRawEvent objects.
 */
class ThriftLoader extends CollectorLoader {
  private val thriftDeserializer = new TDeserializer

  /**
   * Converts the source string into a MaybeCanonicalInput.
   *
   * @param line A serialized Thrift object Byte array mapped to a String.
   *   The method calling this should encode the serialized object
   *   with `snowplowRawEventBytes.map(_.toChar)`.
   *   Reference: http://stackoverflow.com/questions/5250324/
   * @return either a set of validation errors or an Option-boxed
   *         CanonicalInput object, wrapped in a Scalaz ValidatioNel.
   */
  def toCanonicalInput(line: String): ValidatedMaybeCanonicalInput = {
    var snowplowRawEvent = new SnowplowRawEvent
    try {
      thriftDeserializer.deserialize(
        snowplowRawEvent,
        line.toCharArray.map(_.toByte)
      )

      val payload = TrackerPayload.extractGetPayload(
        snowplowRawEvent.payload.data,
        snowplowRawEvent.encoding
      )

      // TODO: There's probably a better way to do this.
      def getOptFromStr(s: String) = if (s == null) None else Some(s)
      val ip = Some(snowplowRawEvent.ipAddress) // Required.
      val hostname = getOptFromStr(snowplowRawEvent.hostname)
      val userAgent = getOptFromStr(snowplowRawEvent.userAgent)
      val refererUri = getOptFromStr(snowplowRawEvent.refererUri)
      val networkUserId = getOptFromStr(snowplowRawEvent.networkUserId)

      (payload.toValidationNel) map { (p:NameValueNel) =>
        Some(
          CanonicalInput(
            new DateTime(snowplowRawEvent.timestamp),
            new NVGetPayload(p),
            InputSource(snowplowRawEvent.collector, hostname),
            snowplowRawEvent.encoding,
            ip,
            userAgent,
            refererUri,
            snowplowRawEvent.headers.toList,
            networkUserId
          )
        )
      }
    } catch {
      // TODO: Check for deserialization errors.
      case _: Throwable =>
        "Line does not match Thrift object.".failNel[Option[CanonicalInput]]
    }
  }
}
