package com.snowplowanalytics.snowplow.collectors.scalastream.metrics

import java.time.Duration

import akka.http.scaladsl.model.{HttpMethod, StatusCode, Uri}
import com.snowplowanalytics.snowplow.collectors.scalastream.metrics.PrometheusMetricsService.Metrics._
import io.prometheus.client.exporter.common.TextFormat
import io.prometheus.client.{CollectorRegistry, Counter, Histogram}
import org.apache.commons.io.output.StringBuilderWriter

trait MetricsService {

  def observeRequest(method: HttpMethod, uri: Uri, status: StatusCode, duration: Duration): Unit

  def report(): String

}

class PrometheusMetricsService() extends MetricsService {

  private val registry = new CollectorRegistry
  private val requestCounter: Counter =
    Counter.build(HttpRequestCount, HttpRequestCountHelp).labelNames(Labels: _*).register(registry)
  private val requestHistogram: Histogram =
    Histogram.build(HttpRequestDuration, HttpRequestDurationHelp).labelNames(Labels: _*).register(registry)

  override def observeRequest(method: HttpMethod, uri: Uri, status: StatusCode, duration: Duration): Unit = {
    val path = uri.path.toString
    val methodValue = method.value
    val code = status.intValue().toString
    requestHistogram.labels(path, methodValue, code).observe(duration.toMillis.toDouble)
    requestCounter.labels(path, methodValue, code).inc()
  }

  override def report(): String = {
    val writer = new StringBuilderWriter()
    TextFormat.write004(writer, registry.metricFamilySamples())
    writer.getBuilder.toString
  }

}

object PrometheusMetricsService {

  object Metrics {
    final val HttpRequestDuration = "http_request_duration_millis"
    final val HttpRequestDurationHelp = "Latency per endpoint"

    final val HttpRequestCount = "http_requests_total"
    final val HttpRequestCountHelp = "Total count of requests to http endpoint"

    final val Labels = Seq("endpoint", "method", "code")
  }

}
