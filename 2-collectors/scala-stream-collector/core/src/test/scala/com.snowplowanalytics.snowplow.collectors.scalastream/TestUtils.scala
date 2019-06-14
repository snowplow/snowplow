/*
 * Copyright (c) 2014-2019 Snowplow Analytics Ltd. All rights reserved.
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
    paths = Map("/com.acme/track" -> "/com.snowplowanalytics.snowplow/tp2", "/com.acme/redirect" -> "/r/tp2", "/com.acme/iglu" -> "/com.snowplowanalytics.iglu/v1"),
    p3p = P3PConfig("/w3c/p3p.xml", "NOI DSP COR NID PSA OUR IND COM NAV STA"),
    CrossDomainConfig(enabled = true, List("*"), secure = false),
    cookie = CookieConfig(true, "sp", 365.days, None, None, secure = false, httpOnly = false, sameSite = None),
    doNotTrackCookie = DoNotTrackCookieConfig(false, "abc", "123"),
    cookieBounce = CookieBounceConfig(false, "bounce", "new-nuid", None),
    redirectMacro = RedirectMacroConfig(false, None),
    rootResponse = RootResponseConfig(false, 404),
    cors = CORSConfig(-1.seconds),
    streams = StreamsConfig(
      good = "good",
      bad = "bad",
      useIpAddressAsPartitionKey = false,
      sink = Kinesis(
        region = "us-east-1",
        threadPoolSize = 12,
        aws = AWSConfig("cpf", "cpf"),
        backoffPolicy = KinesisBackoffPolicyConfig(3000L, 60000L),
        customEndpoint = None
      ),
      buffer = BufferConfig(4000000L, 500L, 60000L)
    ),
    prometheusMetrics = PrometheusMetricsConfig(false, None)
  )
}
