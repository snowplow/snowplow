/*
 * Copyright (c) 2013-2018 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.collectors.scalastream.metrics

import java.time.Duration

import akka.http.scaladsl.model.{HttpMethods, StatusCodes, Uri}
import com.snowplowanalytics.snowplow.collectors.scalastream.model.PrometheusMetricsConfig
import org.specs2.mutable.Specification

class MetricsServiceSpec extends Specification {

  "Prometheus metrics service" should {
    "return report about the observed requests" in {
      val metricsService = new PrometheusMetricsService(PrometheusMetricsConfig(enabled = true, None))

      metricsService.observeRequest(HttpMethods.POST, Uri("/endpoint"), StatusCodes.BadGateway, Duration.ofSeconds(3))

      val report = metricsService.report()

      report contains
        """# HELP http_requests_total Total count of requests to http endpoint
          |# TYPE http_requests_total counter
          |http_requests_total{endpoint="/endpoint",method="POST",code="502",} 1.0""".stripMargin

      report contains
        """# HELP http_request_duration_seconds Latency per endpoint
          |# TYPE http_request_duration_seconds histogram
          |http_request_duration_seconds_bucket{endpoint="/endpoint",method="POST",code="502",le="0.005",} 0.0
          |http_request_duration_seconds_bucket{endpoint="/endpoint",method="POST",code="502",le="0.01",} 0.0
          |http_request_duration_seconds_bucket{endpoint="/endpoint",method="POST",code="502",le="0.025",} 0.0
          |http_request_duration_seconds_bucket{endpoint="/endpoint",method="POST",code="502",le="0.05",} 0.0
          |http_request_duration_seconds_bucket{endpoint="/endpoint",method="POST",code="502",le="0.075",} 0.0
          |http_request_duration_seconds_bucket{endpoint="/endpoint",method="POST",code="502",le="0.1",} 0.0
          |http_request_duration_seconds_bucket{endpoint="/endpoint",method="POST",code="502",le="0.25",} 0.0
          |http_request_duration_seconds_bucket{endpoint="/endpoint",method="POST",code="502",le="0.5",} 0.0
          |http_request_duration_seconds_bucket{endpoint="/endpoint",method="POST",code="502",le="0.75",} 0.0
          |http_request_duration_seconds_bucket{endpoint="/endpoint",method="POST",code="502",le="1.0",} 0.0
          |http_request_duration_seconds_bucket{endpoint="/endpoint",method="POST",code="502",le="2.5",} 0.0
          |http_request_duration_seconds_bucket{endpoint="/endpoint",method="POST",code="502",le="5.0",} 1.0
          |http_request_duration_seconds_bucket{endpoint="/endpoint",method="POST",code="502",le="7.5",} 1.0
          |http_request_duration_seconds_bucket{endpoint="/endpoint",method="POST",code="502",le="10.0",} 1.0
          |http_request_duration_seconds_bucket{endpoint="/endpoint",method="POST",code="502",le="+Inf",} 1.0
          |http_request_duration_seconds_count{endpoint="/endpoint",method="POST",code="502",} 1.0
          |http_request_duration_seconds_sum{endpoint="/endpoint",method="POST",code="502",} 3.0""".stripMargin
    }
  }

}
