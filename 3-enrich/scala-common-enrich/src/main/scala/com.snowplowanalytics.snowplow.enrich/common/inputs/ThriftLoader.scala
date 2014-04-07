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

// Apache Commons
import org.apache.commons.lang3.StringUtils

// Joda-Time
import org.joda.time.{DateTime, DateTimeZone}

// Thrift
import org.apache.thrift.TDeserializer

// Java conversions
import scala.collection.JavaConversions._

// Scalaz
import scalaz._
import Scalaz._

// Snowplow
import com.snowplowanalytics.snowplow.collectors.thrift.{
  SnowplowRawEvent,
  TrackerPayload => ThriftTrackerPayload,
  PayloadProtocol,
  PayloadFormat
}

/**
 * Loader for Thrift SnowplowRawEvent objects.
 */
object ThriftLoader extends CollectorLoader[Array[Byte]] {
  
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
  def toCanonicalInput(line: Array[Byte]): ValidatedMaybeCanonicalInput = {
    
    var snowplowRawEvent = new SnowplowRawEvent()
    try {
      this.synchronized {
        thriftDeserializer.deserialize(
          snowplowRawEvent,
          line
        )
      }

      val payload = TrackerPayload.extractGetPayload(
        Option(snowplowRawEvent.payload.data),
        snowplowRawEvent.encoding
      )

      val ip = snowplowRawEvent.ipAddress.some // Required
      val hostname = Option(snowplowRawEvent.hostname)
      val userAgent = Option(snowplowRawEvent.userAgent)
      val refererUri = Option(snowplowRawEvent.refererUri)
      val networkUserId = Option(snowplowRawEvent.networkUserId)

      val headers = Option(snowplowRawEvent.headers)
        .map(_.toList).getOrElse(Nil)

      (payload.toValidationNel) map { (p: NameValueNel) =>
        Some(
          CanonicalInput(
            new DateTime(snowplowRawEvent.timestamp, DateTimeZone.UTC),
            new NvGetPayload(p),
            InputSource(snowplowRawEvent.collector, hostname),
            snowplowRawEvent.encoding,
            ip,
            userAgent,
            refererUri,
            headers,
            networkUserId
          )
        )
      }
    } catch {
      // TODO: Check for deserialization errors.
      case _: Throwable =>
        "Record does not match Thrift SnowplowRawEvent schema".failNel[Option[CanonicalInput]]
    }
  }
}
