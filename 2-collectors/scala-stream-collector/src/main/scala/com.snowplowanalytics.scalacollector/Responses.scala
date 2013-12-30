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

import com.snowplowanalytics.generated.{
  SnowplowEvent,
  TrackerPayload,
  PayloadProtocol,
  PayloadFormat
}
import com.snowplowanalytics.scalacollector.backends._

import java.util.UUID
import org.apache.commons.codec.binary.Base64
//import org.slf4j.LoggerFactory
import spray.http.{DateTime,HttpResponse,HttpEntity,HttpCookie}
import spray.http.HttpHeaders.{`Set-Cookie`,RawHeader}
import spray.http.MediaTypes.`image/gif`

object Responses {
  val pixel = Base64.decodeBase64("R0lGODlhAQABAAAAACH5BAEKAAEALAAAAAABAAEAAAICTAEAOw==")

  def cookie(queryParams: String, requestCookie: Option[HttpCookie],
      userAgent: Option[String], hostname: String, ip: String) = {
    // Use the same UUID if the request cookie contains `sp`.
    val cookieUUID: String =
      if (requestCookie.isDefined) requestCookie.get.content
      else UUID.randomUUID.toString()

    // Construct an event object from the request.

    // TODO: Should the time be in UTC or local?
    // Should the scema make this more clear?
    val timestamp: Long = System.currentTimeMillis / 1000

    val payload = new TrackerPayload(
      PayloadProtocol.Http,
      PayloadFormat.HttpGet,
      queryParams
    )

    val event = new SnowplowEvent(
      timestamp,
      payload,

      // TODO: Should the collector name/version format be more
      // strictly defined in the schema?
      s"${generated.Settings.name}-${generated.Settings.version}",

      // TODO: should we extract the encoding from the queryParams?
      "UTF-8"
    )

    event.hostname = hostname
    event.ipAddress = ip
    if (userAgent.isDefined) event.userAgent = userAgent.get
    // TODO: Not sure if the refererUri can be easily obtained.
    // event.refererUri = 

    // TODO: Use something like:
    // http://spray.io/documentation/1.1-SNAPSHOT/spray-routing/basic-directives/mapHttpResponseHeaders/
    // to map the HttpResponseHeaders into a list.
    // event.headers = 

    // TODO: Is the user ID the cookie we have associated with a user?
    event.userId = cookieUUID

    if (CollectorConfig.backendEnabledEnum == CollectorConfig.Backend.Kinesis) {
      // TODO: What should the key be?
      KinesisBackend.storeEvent(event, "key")
    } else {
      StdoutBackend.printEvent(event)
    }

    // Build the response.
    val responseCookie = HttpCookie(
      "sp", cookieUUID,
      expires=Some(DateTime.now+CollectorConfig.cookieExpiration),
      domain=CollectorConfig.cookieDomain
    )
    val policyRef = CollectorConfig.p3pPolicyRef
    val CP = CollectorConfig.p3pCP
    val headers = List(
      RawHeader("P3P", s"""policyref="${policyRef}", CP="${CP}""""),
      `Set-Cookie`(responseCookie)
    )
    HttpResponse(entity = HttpEntity(`image/gif`, pixel))
      .withHeaders(headers)
  }

  def dump = HttpResponse(entity=KinesisBackend.getRecordsString)

  def notFound = HttpResponse(status = 404, entity = "404 Not found")
  def timeout = HttpResponse(status = 500, entity = s"Request timed out.")
}
