/* 
 * Copyright (c) 2013 SnowPlow Analytics Ltd. All rights reserved.
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

 // Reference: https://github.com/spray/spray/blob/master/examples/spray-can/simple-http-server/src/main/scala/spray/examples/DemoService.scala

package snowplow.scala_collector

import scala.concurrent.duration._
import akka.pattern.ask
import akka.util.Timeout
import akka.actor._
import spray.can.Http
import spray.can.server.Stats
import spray.util._
import spray.http._
import HttpMethods._
import MediaTypes._
import spray.can.Http.RegisterChunkHandler

class CollectorService extends Actor with ActorLogging {
  implicit val timeout: Timeout = 1.second // For the actor 'asks'
  import context.dispatcher // ExecutionContext for the futures and scheduler.

  // TODO: Currently, requests will be handled sequentially and will
  // cause `receive` to block until a request is handled.
  // Ideally, this should spawn off actors to handle requests.
  // Use something like Spray routing
  // (http://spray.io/documentation/1.2.0/spray-routing/)
  // to route futures to fix this.
  def receive = {
    // Handle connections.
    case _: Http.Connected => sender ! Http.Register(self)

    case HttpRequest(GET, Uri.Path("/i"), headers, content, protocol) =>
      sender ! Responses.cookie

    case HttpRequest(GET, Uri.Path("/stop"), _, _, _)
        if !generated.Settings.production =>
      sender ! Responses.stop
      sender ! Http.Close
      context.system.scheduler.scheduleOnce(1.second)
        { context.system.shutdown() }

    case _: HttpRequest => sender ! Responses.notFound

    case Timedout(HttpRequest(method, uri, _, _, _)) =>
      sender ! Responses.timeout(method, uri)
  }
}
