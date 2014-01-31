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

// Akka
import akka.actor.{Actor,ActorRefFactory}
import akka.pattern.ask
import akka.util.Timeout

// Spray
import spray.http.{Uri,Timedout,HttpRequest}
import spray.routing.HttpService

// Scala
import scala.concurrent.duration._

// Snowplow
import sinks._

// Actor accepting Http requests for the Scala collector.
class CollectorServiceActor(collectorConfig: CollectorConfig,
    sink: AbstractSink) extends Actor with HttpService {
  implicit val timeout: Timeout = 1.second // For the actor 'asks'
  def actorRefFactory = context

  // Deletage responses (content and storing) to the ResponseHandler.
  private val responseHandler = new ResponseHandler(collectorConfig, sink)

  // Use CollectorService so the same route can be accessed differently
  // in the testing framework.
  private val collectorService = new CollectorService(responseHandler, context)

  // Message loop for the Spray service.
  def receive = handleTimeouts orElse runRoute(collectorService.collectorRoute)

  def handleTimeouts: Receive = {
    case Timedout(_) => sender ! responseHandler.timeout
  }
}

// Store the route in CollectorService to be accessed from
// both CollectorServiceActor and from the testing framework.
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
    }~
    complete(responseHandler.notFound)
  }
}
