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
package com.snowplowanalytics.snowplow.collectors
package scalastream

// Akka
import akka.actor.{ Actor, ActorRefFactory, Props }
import akka.pattern.ask
import akka.util.Timeout

// Spray
import spray.http.{ Timedout, HttpRequest }
import spray.routing.HttpService
import spray.routing.Route

// Scala
import scala.concurrent.duration._

// Snowplow
import sinks._

/**
 * Handles request for the scala collector
 */
object CollectorHandler {
  def props(config: CollectorConfig, sink: AbstractSink): Props = Props(classOf[CollectorHandler], config, sink)
}

class CollectorHandler(collectorConfig: CollectorConfig, sink: AbstractSink)
    extends Actor with CollectorService {

  implicit val timeout: Timeout = 1.second

  def actorRefFactory: ActorRefFactory = context

  override val responseHandler = new ResponseHandler(collectorConfig, sink)

  def receive: Receive = handleTimeouts orElse runRoute(collectorRoute)

  def handleTimeouts: Receive = {
    case Timeout(_) => sender ! responseHandler.timeout
  }
}

/**
 * Companion object for the CollectorService class
 */
object CollectorService {
  protected[scalastream] val QuerystringExtractor = "^[^?]*\\?([^#]*)(?:#.*)?$".r
}

/**
 * Stores the route in CollectorService to be accessed from
 * both CollectorServiceActor and from the testing framework.
 */
trait CollectorService extends HttpService {
  import spray.http.HttpMethods

  val responseHandler: ResponseHandler

  val theseParameters = optionalCookie("sp") &
    optionalHeaderValueByName("User-Agent") &
    optionalHeaderValueByName("Referer") &
    headerValueByName("Raw-Request-URI") &
    hostName &
    clientIP &
    requestInstance

  val segmentPath = path(Segment / Segment) & (post | get)
  val somePath = path("""ice\.png""".r | "i".r) & get

  val requestMethod = extract(_.request.method)

  def segmentCollectRoute: Route = {
    segmentPath { (path1, path2) =>
      theseParameters { (reqCookie, userAgent, refererURI, rawRequest, host, ip, request) =>
        requestMethod { m =>
          if (m == HttpMethods.GET) {
            complete(
              responseHandler.cookie(
                rawRequest match {
                  case CollectorService.QuerystringExtractor(qs) => Some(qs)
                  case _                                         => Some("")
                },
                None,
                reqCookie,
                userAgent,
                host,
                ip.toString,
                request,
                refererURI,
                "/" + path1 + "/" + path2,
                true)._1)
          } else {
            entity(as[String]) { body =>
              complete(
                responseHandler.cookie(
                  None,
                  Some(body),
                  reqCookie,
                  userAgent,
                  host,
                  ip.toString,
                  request,
                  refererURI,
                  "/" + path1 + "/" + path2,
                  false)._1)
            }
          }
        }
      }
    }
  }

  def collectorRoute: Route = segmentCollectRoute ~ {
    somePath { path =>
      theseParameters { (reqCookie, userAgent, refererURI, rawRequest, host, ip, request) =>
        complete(
          responseHandler.cookie(
            rawRequest match {
              case CollectorService.QuerystringExtractor(qs) => Some(qs)
              case _                                         => Some("")
            },
            None,
            reqCookie,
            userAgent,
            host,
            ip.toString,
            request,
            refererURI,
            "/" + path,
            true)._1)
      }
    } ~
      get {
        path("health".r) { path =>
          complete(responseHandler.healthy)
        }
      } ~
      complete(responseHandler.notFound)
  }
}
