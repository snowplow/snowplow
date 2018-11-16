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
