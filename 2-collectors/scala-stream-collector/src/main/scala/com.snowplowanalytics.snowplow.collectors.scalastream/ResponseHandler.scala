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
package com.snowplowanalytics.snowplow.collectors
package scalastream

// Java
import java.nio.ByteBuffer
import java.util.UUID

// Apache Commons
import org.apache.commons.codec.binary.Base64

// Spray
import spray.http.{DateTime,HttpRequest,HttpResponse,HttpEntity,HttpCookie}
import spray.http.HttpHeaders.{
  `Set-Cookie`,
  `Remote-Address`,
  `Raw-Request-URI`,
  RawHeader
}
import spray.http.MediaTypes.`image/gif`

// Typesafe config
import com.typesafe.config.Config

// Java conversions
import scala.collection.JavaConversions._

// Snowplow
import generated._
import thrift._
import sinks._

// Contains an invisible pixel to return for `/i` requests.
object ResponseHandler {
  val pixel = Base64.decodeBase64(
    "R0lGODlhAQABAAAAACH5BAEKAAEALAAAAAABAAEAAAICTAEAOw=="
  )
}

// Receive requests and store data into an output sink.
class ResponseHandler(config: CollectorConfig, sink: AbstractSink) {
  
  val Collector = s"${generated.Settings.shortName}-${generated.Settings.version}-" + config.sinkEnabled.toString.toLowerCase

  // When `/i` is requested, this is called and stores an event in the
  // Kinisis sink and returns an invisible pixel with a cookie.
  def cookie(queryParams: String, requestCookie: Option[HttpCookie],
      userAgent: Option[String], hostname: String, ip: String,
      request: HttpRequest, refererUri: Option[String]):
      (HttpResponse, Array[Byte]) = {
    // Use the same UUID if the request cookie contains `sp`.
    val networkUserId: String =
      if (requestCookie.isDefined) requestCookie.get.content
      else UUID.randomUUID.toString()

    // Construct an event object from the request.
    val timestamp: Long = System.currentTimeMillis

    val payload = new TrackerPayload(
      PayloadProtocol.Http,
      PayloadFormat.HttpGet,
      queryParams
    )

    val event = new SnowplowRawEvent(
      timestamp,
      Collector,
      "UTF-8",
      ip
    )

    event.payload = payload
    event.hostname = hostname
    if (userAgent.isDefined) event.userAgent = userAgent.get
    if (refererUri.isDefined) event.refererUri = refererUri.get
    event.headers = request.headers.flatMap {
      case _: `Remote-Address` | _: `Raw-Request-URI` => None
      case other => Some(other.toString)
    }
    event.networkUserId = networkUserId

    // Only the test sink responds with the serialized object.
    val sinkResponse = sink.storeRawEvent(event, ip)

    // Build the HTTP response.
    val responseCookie = HttpCookie(
      "sp", networkUserId,
      expires=Some(DateTime.now+config.cookieExpiration),
      domain=config.cookieDomain
    )
    val policyRef = config.p3pPolicyRef
    val CP = config.p3pCP
    val headers = List(
      RawHeader("P3P", s"""policyref="${policyRef}", CP="${CP}""""),
      `Set-Cookie`(responseCookie)
    )
    val httpResponse = HttpResponse(
      entity = HttpEntity(`image/gif`, ResponseHandler.pixel)
    ).withHeaders(headers)
    (httpResponse, sinkResponse)
  }

  def notFound = HttpResponse(status = 404, entity = "404 Not found")
  def timeout = HttpResponse(status = 500, entity = s"Request timed out.")
}
