/*
 * Copyright (c) 2013-2014 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow
package collectors
package scalastream

// Akka
import akka.actor.{Actor,ActorRefFactory}
import akka.util.Timeout

// Spray
import spray.http.Timedout
import spray.http.HttpCookie
import spray.routing.HttpService
import spray.routing.Directive1

// Scala
import scala.concurrent.duration._

// Snowplow
import model._

// Actor accepting Http requests for the Scala collector.
class CollectorServiceActor(collectorConfig: CollectorConfig,
    sinks: CollectorSinks) extends Actor with HttpService {
  implicit val timeout: Timeout = 1.second // For the actor 'asks'
  def actorRefFactory = context

  // Deletage responses (content and storing) to the ResponseHandler.
  private val responseHandler = new ResponseHandler(collectorConfig, sinks)

  // Use CollectorService so the same route can be accessed differently
  // in the testing framework.
  private val collectorService = new CollectorService(collectorConfig, responseHandler, context)

  // Message loop for the Spray service.
  def receive = handleTimeouts orElse runRoute(collectorService.collectorRoute)

  def handleTimeouts: Receive = {
    case Timedout(_) => sender ! responseHandler.timeout
  }
}

/**
 * Companion object for the CollectorService class
 */
object CollectorService {
  private val QuerystringExtractor = "^[^?]*\\?([^#]*)(?:#.*)?$".r
}

// Store the route in CollectorService to be accessed from
// both CollectorServiceActor and from the testing framework.
class CollectorService(
    collectorConfig: CollectorConfig,
    responseHandler: ResponseHandler,
    context: ActorRefFactory) extends HttpService {
  def actorRefFactory = context

  val cookieName = collectorConfig.cookieName

  // TODO: reduce code duplication here
  val collectorRoute = {
    post {
      path(Segment / Segment) { (path1, path2) =>
        cookieIfWanted(cookieName) { reqCookie =>
          optionalHeaderValueByName("User-Agent") { userAgent =>
            optionalHeaderValueByName("Referer") { refererURI =>
              headerValueByName("Raw-Request-URI") { rawRequest =>
                hostName { host =>
                  clientIP { ip =>
                    requestInstance{ request =>
                      entity(as[String]) { body =>
                        complete(
                          responseHandler.cookie(
                            null,
                            body,
                            reqCookie,
                            userAgent,
                            host,
                            ip,
                            request,
                            refererURI,
                            "/" + path1 + "/" + path2,
                            false
                          )._1
                        )
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    } ~
    get {
      path("""ice\.png""".r | "i".r) { path =>
        cookieIfWanted(cookieName) { reqCookie =>
          optionalHeaderValueByName("User-Agent") { userAgent =>
            optionalHeaderValueByName("Referer") { refererURI =>
              headerValueByName("Raw-Request-URI") { rawRequest =>
                hostName { host =>
                  clientIP { ip =>
                    requestInstance{ request =>
                      complete(
                        responseHandler.cookie(
                          rawRequest match {
                            case CollectorService.QuerystringExtractor(qs) => qs
                            case _ => ""
                          },
                          null,
                          reqCookie,
                          userAgent,
                          host,
                          ip,
                          request,
                          refererURI,
                          "/" + path,
                          true
                        )._1
                      )
                    }
                  }
                }
              }
            }
          }
        }
      }
    } ~
    get {
      path("health".r) { path =>
        complete(responseHandler.healthy)
      }
    } ~
    get {
      path(Segment / Segment) { (path1, path2) =>
        cookieIfWanted(cookieName) { reqCookie =>
          optionalHeaderValueByName("User-Agent") { userAgent =>
            optionalHeaderValueByName("Referer") { refererURI =>
              headerValueByName("Raw-Request-URI") { rawRequest =>
                hostName { host =>
                  clientIP { ip =>
                    requestInstance{ request =>
                      complete(
                        responseHandler.cookie(
                          rawRequest match {
                            case CollectorService.QuerystringExtractor(qs) => qs
                            case _ => ""
                          },
                          null,
                          reqCookie,
                          userAgent,
                          host,
                          ip,
                          request,
                          refererURI,
                          "/" + path1 + "/" + path2,
                          true
                        )._1
                      )
                    }
                  }
                }
              }
            }
          }
        }
      }
    } ~
    options {
      requestInstance { request =>
        complete(responseHandler.preflightResponse(request))
      }
    } ~
    get {
      path("""crossdomain\.xml""".r) { path =>
        complete(responseHandler.flashCrossDomainPolicy)
      }
    } ~
    complete(responseHandler.notFound)
  }

  /**
   * Directive to extract a cookie if a cookie name is specified and if such a cookie exists
   * 
   * @param name Optionally configured cookie name
   * @return Directive1[Option[HttpCookie]]
   */
  def cookieIfWanted(name: Option[String]): Directive1[Option[HttpCookie]] = name match {
    case Some(n) => optionalCookie(n)
    case None => optionalHeaderValue(x => None)
  }
}
