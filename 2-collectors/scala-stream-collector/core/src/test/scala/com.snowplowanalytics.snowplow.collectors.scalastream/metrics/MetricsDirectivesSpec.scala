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

import java.time.Duration

import akka.http.scaladsl.model.{HttpMethod, StatusCode, Uri}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.testkit.Specs2RouteTest
import org.specs2.mutable.Specification

import scala.language.reflectiveCalls

class MetricsDirectivesSpec extends Specification with Specs2RouteTest {

  "The log request directive" should {
    "write data about request and return response" in {
      val service = new MetricsService {
        var observed = false
        override def observeRequest(method: HttpMethod, uri: Uri, status: StatusCode, duration: Duration): Unit =
          observed = true
        override def report(): String = ""
      }
      val directives = new MetricsDirectives {
        override def metricsService: MetricsService = service
      }
      val route = directives.logRequest {
        path("any") {
          complete("ok")
        }
      }

      Get("/any") ~> route ~> check {
        responseAs[String] shouldEqual "ok"
        service.observed shouldEqual true
      }
    }

  }

}
