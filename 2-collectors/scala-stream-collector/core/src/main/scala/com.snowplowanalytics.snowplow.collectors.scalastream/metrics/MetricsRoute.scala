package com.snowplowanalytics.snowplow.collectors.scalastream.metrics

import akka.http.scaladsl.model.{ContentType, HttpEntity, HttpResponse, StatusCodes}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.util.ByteString
import io.prometheus.client.exporter.common.TextFormat

trait MetricsRoute {

  def metricsService: MetricsService

  def metricsRoute: Route =
    (path("metrics") & get) {
      complete(HttpResponse(
        StatusCodes.OK,
        entity = HttpEntity.Strict(MetricsRoute.CONTENT_TYPE_004, ByteString(metricsService.report()))
      ))
    }

}


object MetricsRoute {

  val CONTENT_TYPE_004: ContentType = ContentType.parse(TextFormat.CONTENT_TYPE_004).right.get

}
