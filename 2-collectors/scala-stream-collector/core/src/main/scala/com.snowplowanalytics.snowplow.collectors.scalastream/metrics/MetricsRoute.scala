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
package com.snowplowanalytics.snowplow.collectors.scalastream.metrics

import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.util.ByteString

trait MetricsRoute {
  def metricsService: MetricsService

  def metricsRoute: Route =
    (path("metrics") & get) {
      complete(HttpResponse(
        StatusCodes.OK,
        entity = HttpEntity.Strict(MetricsRoute.`text/plain(UTF-8) v0.0.4`, ByteString(metricsService.report()))
      ))
    }
}

object MetricsRoute {
  val `text/plain(UTF-8) v0.0.4`: ContentType.WithCharset =
    MediaTypes.`text/plain` withParams Map("version" -> "0.0.4") withCharset HttpCharsets.`UTF-8`
}
