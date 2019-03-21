/*
 * Copyright (c) 2012-2019 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.enrich.common.utils

import scala.util.control.NonFatal

import cats.syntax.either._
import scalaj.http._

object HttpClient {

  // The defaults are from scalaj library
  val DEFAULT_CONNECTION_TIMEOUT_MS = 1000
  val DEFAULT_READ_TIMEOUT_MS       = 5000

  /**
   * Blocking method to get body of HTTP response
   * @param request assembled request object
   * @return validated body of HTTP request
   */
  def getBody(request: HttpRequest): Either[Throwable, String] =
    try {
      val res = request.asString
      if (res.isSuccess) res.body.asRight
      else new Exception(s"Request failed with status ${res.code} and body ${res.body}").asLeft
    } catch {
      case NonFatal(e) => e.asLeft
    }

  /**
   * Build HTTP request object
   * @param uri full URI to request
   * @param authUser optional username for basic auth
   * @param authPassword optional password for basic auth
   * @param body optional request body
   * @param method HTTP method
   * @param connectionTimeout connection timeout, if not set default is 1000ms
   * @param readTimeout read timeout, if not set default is 5000ms
   * @return HTTP request
   */
  def buildRequest(
    uri: String,
    authUser: Option[String],
    authPassword: Option[String],
    body: Option[String],
    method: String = "GET",
    connectionTimeout: Option[Long],
    readTimeout: Option[Long]
  ): HttpRequest = {
    val req: HttpRequest = Http(uri).method(method).maybeTimeout(connectionTimeout, readTimeout)
    req.maybeAuth(authUser, authPassword).maybePostData(body)
  }

  implicit class RichHttpRequest(request: HttpRequest) {

    def maybeAuth(user: Option[String], password: Option[String]): HttpRequest =
      if (user.isDefined || password.isDefined)
        request.auth(user.getOrElse(""), password.getOrElse(""))
      else request

    def maybeTimeout(connectionTimeout: Option[Long], readTimeout: Option[Long]): HttpRequest =
      (connectionTimeout, readTimeout) match {
        case (Some(ct), Some(rt)) => request.timeout(ct.toInt, rt.toInt)
        case (Some(ct), None)     => request.timeout(ct.toInt, DEFAULT_READ_TIMEOUT_MS)
        case (None, Some(rt))     => request.timeout(DEFAULT_CONNECTION_TIMEOUT_MS, rt.toInt)
        case _                    => request.timeout(DEFAULT_CONNECTION_TIMEOUT_MS, DEFAULT_READ_TIMEOUT_MS)
      }

    def maybePostData(body: Option[String]): HttpRequest =
      body
        .map(data => request.postData(data).header("content-type", "application/json"))
        .getOrElse(request)
  }
}
