/*
 * Copyright (c) 2014-2016 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.collectors.scalastream

import com.typesafe.config.ConfigFactory

object TestUtils {
   val testConf = ConfigFactory.parseString("""
    collector {
      interface = "0.0.0.0"
      port = 8080

      production = true

      p3p {
        policyref = "/w3c/p3p.xml"
        CP = "NOI DSP COR NID PSA OUR IND COM NAV STA"
      }

      cookie {
        enabled = true
        expiration = 365 days
        name = sp
        domain = "test-domain.com"
      }

      sink {
        enabled = "test"

        kinesis {
          aws {
            access-key: "cpf"
            secret-key: "cpf"
          }
          stream {
            region: "us-east-1"
            good: "snowplow_collector_example"
            bad: "snowplow_collector_example"
          }
          backoffPolicy {
            minBackoff: 3000 # 3 seconds
            maxBackoff: 600000 # 5 minutes
          }
        }

        kafka {
          brokers: "localhost:9092"

          topic {
            good: "good-topic"
            bad: "bad-topic"
          }
        }

        buffer {
          byte-limit: 4000000 # 4MB
          record-limit: 500 # 500 records
          time-limit: 60000 # 1 minute
        }
      }
    }
    """)
}