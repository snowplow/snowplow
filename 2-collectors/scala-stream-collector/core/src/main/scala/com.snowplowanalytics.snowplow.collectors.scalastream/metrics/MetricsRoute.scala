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
