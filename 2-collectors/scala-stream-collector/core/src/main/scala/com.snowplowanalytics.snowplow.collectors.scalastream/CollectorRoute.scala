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

import akka.http.scaladsl.model.{ContentType, HttpResponse}
import akka.http.scaladsl.model.headers.HttpCookiePair
import akka.http.scaladsl.server.{Directive1, Route}
import akka.http.scaladsl.server.Directives._

trait CollectorRoute {
  def collectorService: Service

  private val headers = optionalHeaderValueByName("User-Agent") &
    optionalHeaderValueByName("Referer") &
    optionalHeaderValueByName("Raw-Request-URI")

  private val extractors = extractHost & extractClientIP & extractRequest

  def extractContentType: Directive1[ContentType] =
    extractRequestContext.map(_.request.entity.contentType)

  def collectorRoute: Route =
    cookieIfWanted(collectorService.cookieName) { reqCookie =>
      val cookie = reqCookie.map(_.toCookie)
      headers { (userAgent, refererURI, rawRequestURI) =>
        val qs = queryString(rawRequestURI)
        extractors { (host, ip, request) =>
          // get the adapter vendor and version from the path
          path(Segment / Segment) { (vendor, version) =>
            val path = s"/$vendor/$version"
            post {
              extractContentType { ct =>
                entity(as[String]) { body =>
                  complete {
                    collectorService.cookie(qs, Some(body),
                      path, cookie, userAgent, refererURI, host, ip, request, false, Some(ct))._1
                  }
                }
              }
            } ~
            get {
              complete { collectorService.cookie(
                qs, None, path, cookie, userAgent, refererURI, host, ip, request, true)._1
              }
            }
          } ~
          path("""ice\.png""".r | "i".r) { path =>
            get {
              complete { collectorService.cookie(
                qs, None, "/" + path, cookie, userAgent, refererURI, host, ip, request, true)._1
              }
            }
          }
        }
      }
    } ~ corsRoute ~ healthRoute ~ crossDomainRoute ~
      complete(HttpResponse(404, entity = "404 not found"))

  /**
   * Extract the query string from a request URI
   * @param rawRequestURI URI optionally extracted from the Raw-Request-URI header
   * @return the extracted query string or an empty string
   */
  def queryString(rawRequestURI: Option[String]): Option[String] = {
    val querystringExtractor = "^[^?]*\\?([^#]*)(?:#.*)?$".r
    rawRequestURI.flatMap {
      case querystringExtractor(qs) => Some(qs)
      case _ => None
    }
  }

  /**
   * Directive to extract a cookie if a cookie name is specified and if such a cookie exists
   * @param name Optionally configured cookie name
   */
  def cookieIfWanted(name: Option[String]): Directive1[Option[HttpCookiePair]] = name match {
    case Some(n) => optionalCookie(n)
    case None => optionalHeaderValue(x => None)
  }

  private def crossDomainRoute: Route = get {
    path("""crossdomain\.xml""".r) { path =>
      complete(collectorService.flashCrossDomainPolicy)
    }
  }

  private def healthRoute: Route = get {
    path("health".r) { path =>
      complete(HttpResponse(200, entity = "OK"))
    }
  }

  private def corsRoute: Route = options {
    extractRequest { request =>
      complete(collectorService.preflightResponse(request))
    }
  }
}