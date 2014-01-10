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

// Commons Codec.
import org.apache.commons.codec.binary.Base64

// Joda-Time
import org.joda.time.DateTime

// Thrift.
import org.apache.thrift.TDeserializer

// Specs2.
import org.specs2.mutable._

class ThriftLoaderSpec extends Specification {
  private val thriftDeserializer = new TDeserializer
  private val snowplowRawEventBytes = Base64.decodeBase64("CgABAAABQ3KVZkgMAAoIAAEAAAABCAACAAAAAQsAAwAAABh0ZXN0UGFyYW09MyZ0ZXN0UGFyYW0yPTQACwAUAAAAEHNzYy0wLjAuMS1zdGRvdXQLAB4AAAAFVVRGLTgLACgAAAAJMTI3LjAuMC4xCwApAAAACTEyNy4wLjAuMQsAMgAAAGhNb3ppbGxhLzUuMCAoWDExOyBMaW51eCB4ODZfNjQpIEFwcGxlV2ViS2l0LzUzNy4zNiAoS0hUTUwsIGxpa2UgR2Vja28pIENocm9tZS8zMS4wLjE2NTAuNjMgU2FmYXJpLzUzNy4zNg8ARgsAAAAHAAAAL0Nvb2tpZTogc3A9YzVmM2EwOWYtNzVmOC00MzA5LWJlYzUtZmVhNTYwZjc4NDU1AAAAHkFjY2VwdC1MYW5ndWFnZTogZW4tVVMsIGVuLCBldAAAACRBY2NlcHQtRW5jb2Rpbmc6IGd6aXAsIGRlZmxhdGUsIHNkY2gAAAB0VXNlci1BZ2VudDogTW96aWxsYS81LjAgKFgxMTsgTGludXggeDg2XzY0KSBBcHBsZVdlYktpdC81MzcuMzYgKEtIVE1MLCBsaWtlIEdlY2tvKSBDaHJvbWUvMzEuMC4xNjUwLjYzIFNhZmFyaS81MzcuMzYAAABWQWNjZXB0OiB0ZXh0L2h0bWwsIGFwcGxpY2F0aW9uL3hodG1sK3htbCwgYXBwbGljYXRpb24veG1sO3E9MC45LCBpbWFnZS93ZWJwLCAqLyo7cT0wLjgAAAAWQ29ubmVjdGlvbjoga2VlcC1hbGl2ZQAAABRIb3N0OiAxMjcuMC4wLjE6ODA4MAsAUAAAACRjNWYzYTA5Zi03NWY4LTQzMDktYmVjNS1mZWE1NjBmNzg0NTUA")
  private var snowplowRawEvent = new SnowplowRawEvent
  thriftDeserializer.deserialize(snowplowRawEvent, snowplowRawEventBytes)

  private val thriftLoader = new ThriftLoader

  "Thrift SnowplowRawEvent canonical objects" should {
    val canonicalEvent = thriftLoader.toCanonicalInput(
      new String(snowplowRawEventBytes.map(_.toChar))
    ).toOption.get.get
    "correctly load original parameters" in {
      canonicalEvent.timestamp must beEqualTo(
        new DateTime(snowplowRawEvent.timestamp)
      )
      canonicalEvent.encoding must beEqualTo(snowplowRawEvent.encoding)
    }
  }
}
