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

package com.snowplowanalytics.scala_collector

import akka.actor._
import org.slf4j.LoggerFactory
import spray.http._
import spray.http.HttpHeaders._
import spray.http.MediaTypes._
import HttpMethods._
import org.apache.commons.codec.binary.Base64
import java.util.UUID

object Responses {
  val pixel = Base64.decodeBase64(
    "R0lGODlhAQABAAAAACH5BAEKAAEALAAAAAABAAEAAAICTAEAOw==")
  def cookie(queryParams: Map[String,String]) = {
    val response = HttpResponse(entity = HttpEntity(`image/gif`, pixel))
    val cookie = HttpCookie(
      "sp", UUID.randomUUID.toString()
      // TODO: Expiration.
    )
    val headers = List(`Set-Cookie`(cookie))
    response.withHeaders(headers)
  }

  def notFound() = HttpResponse(status = 404, entity = "404 Not found")
  def timeout(method: HttpMethod, uri: Uri) = HttpResponse(
    status = 500,
    entity = s"The $method request to '$uri' has timed out."
  )
  def stop() = HttpResponse(entity = "Shutting down in 1 second ...")
}
