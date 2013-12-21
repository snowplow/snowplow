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

import java.util.UUID
import org.apache.commons.codec.binary.Base64
//import org.slf4j.LoggerFactory
import spray.http.{DateTime,HttpResponse,HttpEntity,HttpCookie}
import spray.http.HttpHeaders.`Set-Cookie`
import spray.http.MediaTypes.`image/gif`

object Responses {
  val pixel = Base64.decodeBase64("R0lGODlhAQABAAAAACH5BAEKAAEALAAAAAABAAEAAAICTAEAOw==")

  def cookie(queryParams: Map[String,String], reqCookie: Option[String]) = {
    // Use the same UUID if the request cookie contains `sp`.
    var cookieUUID: Option[String] = None
    if (reqCookie.isDefined && (reqCookie.get startsWith "sp=")) {
      cookieUUID = Some(reqCookie.get substring 3)
    } else {
      cookieUUID = Some(UUID.randomUUID.toString())
    }

    val cookie = HttpCookie(
      "sp", cookieUUID.get,
      expires=Some(DateTime.now+generated.Settings.cookieExpirationMs)
    )
    val headers = List(`Set-Cookie`(cookie))
    val response = HttpResponse(entity = HttpEntity(`image/gif`, pixel))
    response.withHeaders(headers)
  }

  def notFound() = HttpResponse(status = 404, entity = "404 Not found")
  def timeout = HttpResponse(status = 500, entity = s"Request timed out.")
}
