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
