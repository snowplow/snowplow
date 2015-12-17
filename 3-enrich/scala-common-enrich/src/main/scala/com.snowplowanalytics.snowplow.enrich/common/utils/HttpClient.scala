/*
 * Copyright (c) 2012-2016 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
 */
package com.snowplowanalytics.snowplow.enrich
package common
package utils

// Scalaz
import scalaz._
import Scalaz._

// Scala
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.control.NonFatal

// Akka
import akka.actor.ActorSystem

// Akka Streams
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.stream.ActorMaterializer
import akka.http.scaladsl.model.headers.{ Authorization, BasicHttpCredentials }


class HttpClient(actorSystem: ActorSystem) {
  /**
   * Inner client to perform HTTP API requests
   */
  implicit val system = actorSystem
  implicit val context = system.dispatcher
  implicit val materializer = ActorMaterializer()

  private val http = Http()

  /**
   * Blocking method to get body of HTTP response
   *
   * @param request assembled request object
   * @param timeout time in milliseconds after which request can be considered failed
   *                used for both connection and receiving
   * @return validated body of HTTP request
   */
  def getBody(request: HttpRequest, timeout: Int): Validation[Throwable, String] = {
    try {
      val response = http.singleRequest(request)
      val body = response.flatMap(_.entity.toStrict(timeout.milliseconds).map(_.data.utf8String))
      Await.result(body, timeout.milliseconds).success
    } catch {
      case NonFatal(e) => e.failure
    }
  }

  /**
   * Build HTTP request object
   *
   * @param uri full URI to request
   * @param authUser optional username for basic auth
   * @param authPassword optional password for basic auth
   * @param method HTTP method
   * @return successful HTTP request or throwable in case of invalid URI or method
   */
  def buildRequest(
      uri: String,
      authUser: Option[String],
      authPassword: Option[String],
      method: String = "GET"): Validation[Throwable, HttpRequest] = {
    val auth = buildAuthorization(authUser, authPassword)
    try {
      HttpRequest(method = HttpMethods.getForKey(method).get, uri = uri, headers = auth.toList).success
    } catch {
      case NonFatal(e) => e.failure
    }
  }

  /**
   * Build [[Authorization]] header .
   * Unlike predefined behaviour which assumes both `authUser` and `authPassword` must be provided
   * this will work if ANY of `authUser` or `authPassword` provided
   */
  private def buildAuthorization(authUser: Option[String], authPassword: Option[String]): Option[Authorization] =
    if (List(authUser, authPassword).flatten.isEmpty) none
    else Authorization(BasicHttpCredentials(authUser.getOrElse(""), authPassword.getOrElse(""))).some
}
