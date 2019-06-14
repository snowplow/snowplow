/*
 * Copyright (c) 2013-2019 Snowplow Analytics Ltd. All rights reserved.
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
package metrics

import java.io.StringWriter
import java.time.Duration

import akka.http.scaladsl.model.{HttpMethod, StatusCode, Uri}
import io.prometheus.client.exporter.common.TextFormat
import io.prometheus.client.{CollectorRegistry, Counter, Gauge, Histogram}

import generated.BuildInfo
import PrometheusMetricsService.Metrics._
import PrometheusMetricsService.NanosecondsInSecond
import model.PrometheusMetricsConfig

/**
  * Service which is used to keep track of processed http requests
  * and report generation based on collected statistics
  */
trait MetricsService {
  def observeRequest(method: HttpMethod, uri: Uri, status: StatusCode, duration: Duration): Unit
  def report(): String
}

/**
  * Implementation of [[com.snowplowanalytics.snowplow.collectors.scalastream.metrics.MetricsService]]
  * which uses [[https://prometheus.io/]] data format for storing metrics and report generation
  * @param metricsConfig Configuration of metrics format
  */
class PrometheusMetricsService(metricsConfig: PrometheusMetricsConfig) extends MetricsService {

  private val registry = new CollectorRegistry
  private val requestCounter: Counter =
    Counter.build(HttpRequestCount, HttpRequestCountHelp).labelNames(Labels: _*).register(registry)
  private val requestDurationHistogramBuilder = Histogram.build(HttpRequestDuration, HttpRequestDurationHelp).labelNames(Labels: _*)
  private val requestDurationHistogram: Histogram =
    metricsConfig.durationBucketsInSeconds
      .map(buckets => requestDurationHistogramBuilder.buckets(buckets: _*).register(registry))
      .getOrElse(requestDurationHistogramBuilder.register(registry))

  private val applicationVersionGauge = Gauge.build("service_version", "Java, scala versions and collector version")
    .labelNames("java_version", "scala_version", "version")
    .register(registry)

  applicationVersionGauge.labels(System.getProperty("java.version"), BuildInfo.scalaVersion, BuildInfo.version).set(1)

  override def observeRequest(method: HttpMethod, uri: Uri, status: StatusCode, duration: Duration): Unit = {
    val path = uri.path.toString
    val methodValue = method.value
    val code = status.intValue().toString
    requestDurationHistogram.labels(path, methodValue, code).observe(duration.toNanos / NanosecondsInSecond)
    requestCounter.labels(path, methodValue, code).inc()
  }

  override def report(): String = {
    val writer = new StringWriter()
    TextFormat.write004(writer, registry.metricFamilySamples())
    writer.toString
  }
}

object PrometheusMetricsService {
  final val NanosecondsInSecond: Double = Math.pow(10, 9)

  object Metrics {
    final val HttpRequestDuration = "http_request_duration_seconds"
    final val HttpRequestDurationHelp = "Latency per endpoint"

    final val HttpRequestCount = "http_requests_total"
    final val HttpRequestCountHelp = "Total count of requests to http endpoint"

    final val Labels = Seq("endpoint", "method", "code")
  }
}
