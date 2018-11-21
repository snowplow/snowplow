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
