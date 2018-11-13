package com.snowplowanalytics.snowplow.collectors.scalastream.metrics

import java.time.{Duration, LocalDateTime}

import akka.http.scaladsl.server.Directive0
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.directives.BasicDirectives.extractRequestContext

trait MetricsDirectives {

  def metricsService: MetricsService

  val logRequest: Directive0 =
    extractRequestContext.flatMap { ctx =>
      val startTime = LocalDateTime.now()
      mapResponse { response =>
        val requestDuration = Duration.between(startTime, LocalDateTime.now())
        metricsService.observeRequest(ctx.request.method, ctx.request.uri, response.status, requestDuration)
        response
      }
    }

}
