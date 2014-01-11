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

// Commons Codec
import org.apache.commons.codec.binary.Base64

// Joda-Time
import org.joda.time.DateTime

// Thrift
import org.apache.thrift.TDeserializer

// Specs2
import org.specs2.{Specification, ScalaCheck}
import org.specs2.matcher.DataTables
import org.specs2.scalaz.ValidationMatchers

// ScalaCheck
import org.scalacheck._
import org.scalacheck.Arbitrary._

// Snowplow
import com.snowplowanalytics.snowplow.collectors.thrift.{
  SnowplowRawEvent,
  TrackerPayload => ThriftTrackerPayload,
  PayloadProtocol,
  PayloadFormat
}

class ThriftLoaderSpec extends Specification with DataTables with ValidationMatchers with ScalaCheck { def is =

  "This is a specification to test the ThriftLoader functionality"                                          ^
                                                                                                           p^
  "toCanonicalInput should return a CanonicalInput for a valid Thrift SnowplowRawEvent"                     ! e1^
  "toCanonicalInput should return a Validation Failure for an invalid or corrupted Thrift SnowplowRawEvent" ! e2^
                                                                                                            end

  object Expected {
    val encoding = "UTF-8"
  }

  // Conversion
  // TODO: rename after testing on REPL to figure out difference between these two!
  val str2Str: (String) => String = (raw) => {
    val bytes = Base64.decodeBase64(raw)
    new String(bytes.map(_.toChar))
  }

  // TODO: add rest of columns into this one
  // TODO: add more tests into this one
  def e1 =
    "SPEC NAME"                 || "RAW" | "EXP. TIMESTAMP" |
    "Thrift #1"                 !! "CgABAAABQ3KVZkgMAAoIAAEAAAABCAACAAAAAQsAAwAAABh0ZXN0UGFyYW09MyZ0ZXN0UGFyYW0yPTQACwAUAAAAEHNzYy0wLjAuMS1zdGRvdXQLAB4AAAAFVVRGLTgLACgAAAAJMTI3LjAuMC4xCwApAAAACTEyNy4wLjAuMQsAMgAAAGhNb3ppbGxhLzUuMCAoWDExOyBMaW51eCB4ODZfNjQpIEFwcGxlV2ViS2l0LzUzNy4zNiAoS0hUTUwsIGxpa2UgR2Vja28pIENocm9tZS8zMS4wLjE2NTAuNjMgU2FmYXJpLzUzNy4zNg8ARgsAAAAHAAAAL0Nvb2tpZTogc3A9YzVmM2EwOWYtNzVmOC00MzA5LWJlYzUtZmVhNTYwZjc4NDU1AAAAHkFjY2VwdC1MYW5ndWFnZTogZW4tVVMsIGVuLCBldAAAACRBY2NlcHQtRW5jb2Rpbmc6IGd6aXAsIGRlZmxhdGUsIHNkY2gAAAB0VXNlci1BZ2VudDogTW96aWxsYS81LjAgKFgxMTsgTGludXggeDg2XzY0KSBBcHBsZVdlYktpdC81MzcuMzYgKEtIVE1MLCBsaWtlIEdlY2tvKSBDaHJvbWUvMzEuMC4xNjUwLjYzIFNhZmFyaS81MzcuMzYAAABWQWNjZXB0OiB0ZXh0L2h0bWwsIGFwcGxpY2F0aW9uL3hodG1sK3htbCwgYXBwbGljYXRpb24veG1sO3E9MC45LCBpbWFnZS93ZWJwLCAqLyo7cT0wLjgAAAAWQ29ubmVjdGlvbjoga2VlcC1hbGl2ZQAAABRIb3N0OiAxMjcuMC4wLjE6ODA4MAsAUAAAACRjNWYzYTA5Zi03NWY4LTQzMDktYmVjNS1mZWE1NjBmNzg0NTUA" !
                                           new DateTime()   |> {
      (_, raw, timestamp) => {
        val canonicalEvent = ThriftLoader
          .toCanonicalInput(str2Str(raw))
          .toOption.get.get // TODO: clean this up

        canonicalEvent.timestamp must beEqualTo(
          timestamp // TODO: why is this passing?
        )
        canonicalEvent.encoding must beEqualTo(Expected.encoding)
      }
    }

  // A bit of fun: the chances of generating a valid Thrift SnowplowRawEvent at random are
  // so low, that we can just use ScalaCheck here
  def e2 =
    check { (raw: String) => ThriftLoader.toCanonicalInput(str2Str(raw)) must beFailing }
}
