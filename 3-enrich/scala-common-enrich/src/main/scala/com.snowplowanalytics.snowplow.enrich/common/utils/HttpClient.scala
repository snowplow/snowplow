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

import scala.util.control.NonFatal

// Scalaz
import scalaz._
import Scalaz._

// Scalaj
import scalaj.http._

object HttpClient {
  /**
   * Blocking method to get body of HTTP response
   *
   * @param request assembled request object
   * @return validated body of HTTP request
   */
  def getBody(request: HttpRequest): Validation[Throwable, String] = {
    try {
      val res = request.asString
      if (res.isSuccess) res.body.success
      else new Exception(s"Request failed with status ${res.code} and body ${res.body}").failure
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
   * @return HTTP request
   */
  def buildRequest(
    uri: String,
    authUser: Option[String],
    authPassword: Option[String],
    method: String = "GET"
  ): HttpRequest = {
    val req = Http(uri).method(method)
    if (authUser.isDefined || authPassword.isDefined) {
      req.auth(authUser.getOrElse(""), authPassword.getOrElse(""))
    } else {
      req
    }
  }
}
