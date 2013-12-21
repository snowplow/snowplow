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

package com.snowplowanalytics.scalacollector

import akka.actor.Actor
import akka.pattern.ask
import akka.util.Timeout
import scala.concurrent.duration._
import spray.http.Timedout
import spray.routing.HttpService

class CollectorServiceActor extends Actor with CollectorService {
  implicit val timeout: Timeout = 1.second // For the actor 'asks'

  def actorRefFactory = context
  def receive = handleTimeouts orElse runRoute(route)

  // http://spray.io/documentation/1.2.0/spray-routing/key-concepts/timeout-handling/
  def handleTimeouts: Receive = {
    case Timedout(_) => sender ! Responses.timeout
  }
}

trait CollectorService extends HttpService {
  implicit def executionContext = actorRefFactory.dispatcher
  val route = {
    path("i") {
      get {
        parameterMap {
          queryParams =>
            optionalHeaderValueByName("Cookie") { reqCookie =>
              complete(Responses.cookie(queryParams, reqCookie))
            }
        }
      }
    }~
    get {
      complete(Responses.notFound)
    }
  }
}
