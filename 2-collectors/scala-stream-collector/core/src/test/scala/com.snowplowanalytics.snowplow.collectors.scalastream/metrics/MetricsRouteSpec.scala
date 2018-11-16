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

import akka.http.scaladsl.model.{HttpMethod, StatusCode, Uri}
import akka.http.scaladsl.testkit.Specs2RouteTest
import org.specs2.mutable.Specification

class MetricsRouteSpec extends Specification with Specs2RouteTest {

  val route = new MetricsRoute {
    override def metricsService: MetricsService = new MetricsService {
      override def observeRequest(method: HttpMethod, uri: Uri, status: StatusCode, duration: Duration): Unit = {}

      override def report(): String = "some_report"
    }
  }

  "The metrics route" should {
    "respond with metrics from service" in {
      Get("/metrics") ~> route.metricsRoute ~> check {
        responseAs[String] shouldEqual "some_report"
      }
    }
  }

}
