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
import backends._

import akka.actor.Actor
import akka.pattern.ask
import akka.util.Timeout
import scala.concurrent.duration._
import spray.http.Timedout
import spray.routing.HttpService

class CollectorService(collectorConfig: CollectorConfig, kinesisBackend: KinesisBackend) extends Actor with HttpService {
  implicit val timeout: Timeout = 1.second // For the actor 'asks'
  private def responseHandler = new ResponseHandler(collectorConfig, kinesisBackend)

  def actorRefFactory = context
  def receive = handleTimeouts orElse runRoute(collectorRoute)

  def handleTimeouts: Receive = {
    case Timedout(_) => sender ! responseHandler.timeout
  }

  private def paramString(param: (String, String)): String =
    s"${param._1}=${param._2}"

  val collectorRoute = {
    get {
      path("i") {
        parameterSeq { params =>
          optionalCookie("sp") { reqCookie =>
            optionalHeaderValueByName("User-Agent") { userAgent =>
              hostName { host =>
                clientIP { ip =>
                  // Reference: http://spray.io/documentation/1.2.0/spray-routing/parameter-directives/parameterSeq/#parameterseq
                  // TODO: Reconstructing this string doesn't seem best,
                  // but I can't find a better way, so I posted to the
                  // spray mailing list, 2013.12.24.
                  val paramsString = params.map(paramString).mkString("&")
                  complete(responseHandler.cookie(
                    paramsString,
                    reqCookie,
                    userAgent,
                    host,
                    ip.toString
                  ))
                }
              }
            }
          }
        }
      }~
    }~
    complete(responseHandler.notFound)
  }
}
