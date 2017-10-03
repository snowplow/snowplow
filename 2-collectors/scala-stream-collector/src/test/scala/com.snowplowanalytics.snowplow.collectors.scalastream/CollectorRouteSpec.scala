/*
 * Copyright (c) 2013-2017 Snowplow Analytics Ltd. All rights reserved.
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

import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers._
import akka.http.scaladsl.testkit.Specs2RouteTest
import akka.http.scaladsl.server.Directives._
import org.specs2.mutable.Specification

class CollectorRouteSpec extends Specification with Specs2RouteTest {
  val route = new CollectorRoute {
    override val collectorService = new Service {
      def preflightResponse(req: HttpRequest): HttpResponse =
        HttpResponse(200, entity = "preflight response")
      def flashCrossDomainPolicy: HttpResponse = HttpResponse(200, entity = "flash cross domain")
      def cookie(
        queryString: Option[String],
        body: Option[String],
        path: String,
        cookie: Option[HttpCookie],
        userAgent: Option[String],
        refererUri: Option[String],
        hostname: String,
        ip: RemoteAddress,
        request: HttpRequest,
        pixelExpected: Boolean,
        contentType: Option[ContentType] = None
      ): (HttpResponse, List[Array[Byte]]) = (HttpResponse(200, entity = s"cookie"), List.empty)
      def cookieName: Option[String] = Some("name")
    }
  }

  "The collector route" should {
    "respond to the cors route with a preflight response" in {
      Options() ~> route.collectorRoute ~> check {
        responseAs[String] shouldEqual "preflight response"
      }
    }
    "respond to the health route with an ok response" in {
      Get("/health") ~> route.collectorRoute ~> check {
        responseAs[String] shouldEqual "OK"
      }
    }
    "respond to the cross domain route with the cross domain policy" in {
      Get("/crossdomain.xml") ~> route.collectorRoute ~> check {
        responseAs[String] shouldEqual "flash cross domain"
      }
    }
    "respond to the post cookie route with the cookie response" in {
      Post("/p1/p2") ~> route.collectorRoute ~> check {
        responseAs[String] shouldEqual "cookie"
      }
    }
    "respond to the get cookie route with the cookie response" in {
      Get("/p1/p2") ~> route.collectorRoute ~> check {
        responseAs[String] shouldEqual "cookie"
      }
    }
    "respond to the pixel route with the cookie response" in {
      Get("/ice.png") ~> route.collectorRoute ~> check {
        responseAs[String] shouldEqual "cookie"
      }
      Get("/i") ~> route.collectorRoute ~> check {
        responseAs[String] shouldEqual "cookie"
      }
    }
    "respond to anything else with a not found" in {
      Get("/something") ~> route.collectorRoute ~> check {
        responseAs[String] shouldEqual "404 not found"
      }
    }

    "extract a query string" in {
      "produce the query string if present" in {
        route.queryString(Some("/abc/def?a=12&b=13#frg")) shouldEqual Some("a=12&b=13")
      }
      "produce an empty string if the extractor doesn't match" in {
        route.queryString(Some("/abc/def#frg")) shouldEqual None
      }
      "produce an empty string if the argument is None" in {
        route.queryString(None) shouldEqual None
      }
    }

    "have a directive extracting a cookie" in {
      "return the cookie if some cookie name is given" in {
        Get() ~> Cookie("abc" -> "123") ~>
          route.cookieIfWanted(Some("abc")) { c =>
            complete(HttpResponse(200, entity = c.toString))
          } ~> check {
            responseAs[String] shouldEqual "Some(abc=123)"
          }
      }
      "return none if no cookie name is given" in {
        Get() ~> Cookie("abc" -> "123") ~>
          route.cookieIfWanted(None) { c =>
            complete(HttpResponse(200, entity = c.toString))
          } ~> check {
            responseAs[String] shouldEqual "None"
          }
      }
    }
  }
}