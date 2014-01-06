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

import generated._
import thrift._
import backends._

import java.util.UUID
import org.apache.commons.codec.binary.Base64
//import org.slf4j.LoggerFactory
import spray.http.{DateTime,HttpResponse,HttpEntity,HttpCookie}
import spray.http.HttpHeaders.{`Set-Cookie`,RawHeader}
import spray.http.MediaTypes.`image/gif`

import com.typesafe.config.Config

class ResponseHandler(collectorConfig: CollectorConfig, kinesisBackend: KinesisBackend) {
  val pixel = Base64.decodeBase64("R0lGODlhAQABAAAAACH5BAEKAAEALAAAAAABAAEAAAICTAEAOw==")

  def cookie(queryParams: String, requestCookie: Option[HttpCookie],
      userAgent: Option[String], hostname: String, ip: String) = {
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
      payload,
      s"${generated.Settings.shortName}-${generated.Settings.version}-${collectorConfig.backendEnabled}",
      "UTF-8" // TODO: should we extract the encoding from the queryParams?
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

    event.networkUserId = networkUserId

    if (collectorConfig.backendEnabledEnum == collectorConfig.Backend.Kinesis) {
      // TODO: What should the key be?
      kinesisBackend.storeEvent(event, "key")
    } else {
      StdoutBackend.printEvent(event)
    }

    // Build the response.
    val responseCookie = HttpCookie(
      "sp", networkUserId,
      expires=Some(DateTime.now+collectorConfig.cookieExpiration),
      domain=collectorConfig.cookieDomain
    )
    val policyRef = collectorConfig.p3pPolicyRef
    val CP = collectorConfig.p3pCP
    val headers = List(
      RawHeader("P3P", s"""policyref="${policyRef}", CP="${CP}""""),
      `Set-Cookie`(responseCookie)
    )
    HttpResponse(entity = HttpEntity(`image/gif`, pixel))
      .withHeaders(headers)
  }

  def notFound = HttpResponse(status = 404, entity = "404 Not found")
  def timeout = HttpResponse(status = 500, entity = s"Request timed out.")
}
