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

// Config
import com.typesafe.config.{ConfigFactory,Config,ConfigException}

// Snowplow
import sources.TestSource

/**
 * Defines some useful helpers for the specs:
 *
 * 1. The regexp pattern for a Type 4 UUID
 * 2. The indices for CanonicalOutput fields
 *    which contain Type 4 UUIDs
 * 3. A TestSource configured for test source
 *    and test sink
 */
object SpecHelpers {

  /**
   * The regexp pattern for a Type 4 UUID.
   *
   * Taken from Gajus Kuizinas's SO answer:
   * http://stackoverflow.com/a/14166194/255627
   *
   * TODO: should this be a Specs2 contrib?
   */
  val uuid4Regexp = "[a-f0-9]{8}-[a-f0-9]{4}-4[a-f0-9]{3}-[89aAbB][a-f0-9]{3}-[a-f0-9]{12}"

  /**
   * The indices in the CanonicalOutput for
   * fields which are in Type 4 UUIDs.
   */
  val uuid4Fields = List(6)

  /**
   * A TestSource for testing against.
   * Built using an inline configuration file
   * with both source and sink set to test.
   */
  val testSource = {

    val config = """
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
      maxmind_file: "/maxmind/GeoLiteCity.dat" # SBT auto-downloads into resource_managed/test
    }
    anon_ip: {
      enabled: true
      anon_octets: 1 # Or 2, 3 or 4. 0 is same as enabled: false
    }
  }
}
"""

    val conf = ConfigFactory.parseString(config)
    val kec = new KinesisEnrichConfig(conf)

    new TestSource(kec)
  }
}
