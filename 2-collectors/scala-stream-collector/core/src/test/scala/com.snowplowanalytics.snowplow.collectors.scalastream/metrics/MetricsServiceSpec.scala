package com.snowplowanalytics.snowplow.collectors.scalastream.metrics

import java.time.Duration

import akka.http.scaladsl.model.{HttpMethods, StatusCodes, Uri}
import org.specs2.mutable.Specification


class MetricsServiceSpec extends Specification {


  "Prometheus metrics service" should {
    "return report about the observed requests" in {
      val metricsService = new PrometheusMetricsService

      metricsService.observeRequest(HttpMethods.POST, Uri("/endpoint"), StatusCodes.BadGateway, Duration.ofMillis(3))

      metricsService.report() shouldEqual
        """# HELP http_requests_total Total count of requests to http endpoint
           |# TYPE http_requests_total counter
           |http_requests_total{endpoint="/endpoint",method="POST",code="502",} 1.0
           |# HELP http_request_duration_millis Latency per endpoint
           |# TYPE http_request_duration_millis histogram
           |http_request_duration_millis_bucket{endpoint="/endpoint",method="POST",code="502",le="0.005",} 0.0
           |http_request_duration_millis_bucket{endpoint="/endpoint",method="POST",code="502",le="0.01",} 0.0
           |http_request_duration_millis_bucket{endpoint="/endpoint",method="POST",code="502",le="0.025",} 0.0
           |http_request_duration_millis_bucket{endpoint="/endpoint",method="POST",code="502",le="0.05",} 0.0
           |http_request_duration_millis_bucket{endpoint="/endpoint",method="POST",code="502",le="0.075",} 0.0
           |http_request_duration_millis_bucket{endpoint="/endpoint",method="POST",code="502",le="0.1",} 0.0
           |http_request_duration_millis_bucket{endpoint="/endpoint",method="POST",code="502",le="0.25",} 0.0
           |http_request_duration_millis_bucket{endpoint="/endpoint",method="POST",code="502",le="0.5",} 0.0
           |http_request_duration_millis_bucket{endpoint="/endpoint",method="POST",code="502",le="0.75",} 0.0
           |http_request_duration_millis_bucket{endpoint="/endpoint",method="POST",code="502",le="1.0",} 0.0
           |http_request_duration_millis_bucket{endpoint="/endpoint",method="POST",code="502",le="2.5",} 0.0
           |http_request_duration_millis_bucket{endpoint="/endpoint",method="POST",code="502",le="5.0",} 1.0
           |http_request_duration_millis_bucket{endpoint="/endpoint",method="POST",code="502",le="7.5",} 1.0
           |http_request_duration_millis_bucket{endpoint="/endpoint",method="POST",code="502",le="10.0",} 1.0
           |http_request_duration_millis_bucket{endpoint="/endpoint",method="POST",code="502",le="+Inf",} 1.0
           |http_request_duration_millis_count{endpoint="/endpoint",method="POST",code="502",} 1.0
           |http_request_duration_millis_sum{endpoint="/endpoint",method="POST",code="502",} 3.0
           |""".stripMargin
    }
  }

}
