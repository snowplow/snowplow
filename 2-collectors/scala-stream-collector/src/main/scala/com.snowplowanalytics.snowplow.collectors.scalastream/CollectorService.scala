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

package com.snowplowanalytics.snowplow.collectors.scalastream
import sinks._

import akka.actor.{Actor,ActorRefFactory}
import akka.pattern.ask
import akka.util.Timeout
import scala.concurrent.duration._
import spray.http.{Uri,Timedout,HttpRequest}
import spray.routing.HttpService

class CollectorServiceActor(
    collectorConfig: CollectorConfig,
    kinesisSink: KinesisSink) extends Actor with HttpService {
  implicit val timeout: Timeout = 1.second // For the actor 'asks'
  def actorRefFactory = context
  private def responseHandler = new ResponseHandler(
    collectorConfig,
    kinesisSink
  )
  private val collectorService = new CollectorService(responseHandler, context)

  def receive = handleTimeouts orElse runRoute(collectorService.collectorRoute)

  def handleTimeouts: Receive = {
    case Timedout(_) => sender ! responseHandler.timeout
  }
}

class CollectorService(
    responseHandler: ResponseHandler,
    context: ActorRefFactory) extends HttpService {
  def actorRefFactory = context
  val collectorRoute = {
    get {
      path("i") {
        optionalCookie("sp") { reqCookie =>
          optionalHeaderValueByName("User-Agent") { userAgent =>
            optionalHeaderValueByName("Referer") { refererURI =>
              headerValueByName("Raw-Request-URI") { rawRequest =>
                hostName { host =>
                  clientIP { ip =>
                    requestInstance{ request =>
                      complete(
                        responseHandler.cookie(
                          Option(Uri(rawRequest).query.toString).filter(
                            _.trim.nonEmpty
                          ).getOrElse(null),
                          reqCookie,
                          userAgent,
                          host,
                          ip.toString,
                          request,
                          refererURI
                        )
                      )
                    }
                  }
                }
              }
            }
          }
        }
      }
    }~
    complete(responseHandler.notFound)
  }
}
