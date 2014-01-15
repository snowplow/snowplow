/* 
 * Copyright (c) 2013-2014 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0, and
 * you may not use this file except in compliance with the Apache License
 * Version 2.0.  You may obtain a copy of the Apache License Version 2.0 at
 * http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the Apache License Version 2.0 is distributed on an "AS
 * IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the Apache License Version 2.0 for the specific language
 * governing permissions and limitations there under.
 */

package com.snowplowanalytics.snowplow
package enrich.kinesis

// Snowplow
import sources._
import collectors.thrift.{
  PayloadProtocol,
  PayloadFormat,
  SnowplowRawEvent
}

// Commons Codec
import org.apache.commons.codec.binary.Base64

// specs2 and spray testing libraries
import org.specs2.matcher.AnyMatchers
import org.specs2.mutable.Specification
import org.specs2.specification.{Scope,Fragments}

// Config
import com.typesafe.config.{ConfigFactory,Config,ConfigException}

// Thrift
import org.apache.thrift.TDeserializer

class KinesisEnrichSpec extends Specification with AnyMatchers {
  val config = new KinesisEnrichConfig(ConfigFactory.parseString("""
enrich {
  source = "test"
  sink= "test"

  aws {
    access-key: "cpf"
    secret-key: "cpf"
  }

  streams {
    in: {
      raw: "SnowplowRaw"
    }
    out: {
      enriched: "SnowplowEnriched"
      enriched_shards: 1 # Number of shards to use if created.
      bad: "SnowplowBad" # Not used until #463
      bad_shards: 1 # Number of shards to use if created.
    }
    app-name: SnowplowKinesisEnrich-${enrich.streams.in.raw}
    initial-position = "TRIM_HORIZON"
    endpoint: "https://kinesis.us-east-1.amazonaws.com"
  }
  enrichments {
    geo_ip: {
      enabled: true # false not yet suported
      maxmind_file: "/tmp/GeoLiteCity.dat"
    }
    anon_ip: {
      enabled: true
      anon_octets: 1 # Or 2, 3 or 4. 0 is same as enabled: false
    }
  }
}
"""))

  val kinesisSource = new KinesisSource(config)

  "Snowplow's Kinesis enricher" should {
    "enrich a valid SnowplowRawEvent." in {
      val eventBytes = Base64.decodeBase64("CgABAAABQ3KVZkgMAAoIAAEAAAABCAACAAAAAQsAAwAAABh0ZXN0UGFyYW09MyZ0ZXN0UGFyYW0yPTQACwAUAAAAEHNzYy0wLjAuMS1zdGRvdXQLAB4AAAAFVVRGLTgLACgAAAAJMTI3LjAuMC4xCwApAAAACTEyNy4wLjAuMQsAMgAAAGhNb3ppbGxhLzUuMCAoWDExOyBMaW51eCB4ODZfNjQpIEFwcGxlV2ViS2l0LzUzNy4zNiAoS0hUTUwsIGxpa2UgR2Vja28pIENocm9tZS8zMS4wLjE2NTAuNjMgU2FmYXJpLzUzNy4zNg8ARgsAAAAHAAAAL0Nvb2tpZTogc3A9YzVmM2EwOWYtNzVmOC00MzA5LWJlYzUtZmVhNTYwZjc4NDU1AAAAHkFjY2VwdC1MYW5ndWFnZTogZW4tVVMsIGVuLCBldAAAACRBY2NlcHQtRW5jb2Rpbmc6IGd6aXAsIGRlZmxhdGUsIHNkY2gAAAB0VXNlci1BZ2VudDogTW96aWxsYS81LjAgKFgxMTsgTGludXggeDg2XzY0KSBBcHBsZVdlYktpdC81MzcuMzYgKEtIVE1MLCBsaWtlIEdlY2tvKSBDaHJvbWUvMzEuMC4xNjUwLjYzIFNhZmFyaS81MzcuMzYAAABWQWNjZXB0OiB0ZXh0L2h0bWwsIGFwcGxpY2F0aW9uL3hodG1sK3htbCwgYXBwbGljYXRpb24veG1sO3E9MC45LCBpbWFnZS93ZWJwLCAqLyo7cT0wLjgAAAAWQ29ubmVjdGlvbjoga2VlcC1hbGl2ZQAAABRIb3N0OiAxMjcuMC4wLjE6ODA4MAsAUAAAACRjNWYzYTA5Zi03NWY4LTQzMDktYmVjNS1mZWE1NjBmNzg0NTUA")
      kinesisSource.runTest(eventBytes) must beEqualTo("TODO")
    }
  }
}
