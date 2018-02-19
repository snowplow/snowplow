/*
 * Copyright (c) 2014-2017 Snowplow Analytics Ltd. All rights reserved.
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

import scala.concurrent.duration._

import model._

object TestUtils {
  val testConf = CollectorConfig(
    interface = "0.0.0.0",
    port = 8080,
    p3p = P3PConfig("/w3c/p3p.xml", "NOI DSP COR NID PSA OUR IND COM NAV STA"),
    CrossDomainConfig(enabled = true, "*", secure = false),
    cookie = CookieConfig(true, "sp", 365.days, None),
    cookieBounce = CookieBounceConfig(false, "bounce", "new-nuid", None),
    redirectMacro = RedirectMacroConfig(false, None),
    streams = StreamsConfig(
      good = "good",
      bad = "bad",
      useIpAddressAsPartitionKey = false,
      sink = Kinesis(
        region = "us-east-1",
        threadPoolSize = 12,
        aws = AWSConfig("cpf", "cpf"),
        backoffPolicy = BackoffPolicyConfig(3000L, 60000L, 100000L, 1.25d)
      ),
      buffer = BufferConfig(4000000, 500, 60000)
    )
  )
}