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
package com.snowplowanalytics.snowplow
package collectors.scalastream

import java.util.UUID

import scala.collection.JavaConverters._

import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers._
import org.apache.commons.codec.binary.Base64
import org.apache.thrift.TSerializer
import org.slf4j.LoggerFactory
import scalaz._

import CollectorPayload.thrift.model1.CollectorPayload
import enrich.common.outputs.BadRow
import generated.Settings
import model._
import sinks.KinesisSink
import utils.SplitBatch

/**
 * Service responding to HTTP requests, mainly setting a cookie identifying the user and storing
 * events
 */
trait Service {
  def preflightResponse(req: HttpRequest): HttpResponse
  def flashCrossDomainPolicy: HttpResponse
  def cookie(
    queryString: Option[String],
    body: Option[String],
    path: String,
    cookie: Option[HttpCookie],
    userAgent: Option[String],
    refererUri: Option[String],
    hostname: String,
    ip: RemoteAddress,
    request: HttpRequest,
    pixelExpected: Boolean,
    contentType: Option[ContentType] = None
  ): (HttpResponse, List[Array[Byte]])
  def cookieName: Option[String]
}

object CollectorService {
  // Contains an invisible pixel to return for `/i` requests.
  val pixel = Base64.decodeBase64(
    "R0lGODlhAQABAPAAAP///wAAACH5BAEAAAAALAAAAAABAAEAAAICRAEAOw==")
}

class CollectorService(
  config: CollectorConfig,
  sinks: CollectorSinks
) extends Service {

  private val logger = LoggerFactory.getLogger(getClass)

  private val collector =
    s"${Settings.shortName}-${Settings.version}-${config.sink.toString.toLowerCase}"

  override val cookieName = config.cookieName

  override def cookie(
    queryString: Option[String],
    body: Option[String],
    path: String,
    cookie: Option[HttpCookie],
    userAgent: Option[String],
    refererUri: Option[String],
    hostname: String,
    ip: RemoteAddress,
    request: HttpRequest,
    pixelExpected: Boolean,
    contentType: Option[ContentType] = None
  ): (HttpResponse, List[Array[Byte]]) = {
    val (ipAddress, partitionKey) = getIpAndPartitionKey(ip, config.streams.useIpAddressAsPartitionKey)
    val nuid = getNetworkUserId(request, cookie)
    val ct = contentType.map(_.value.toLowerCase)
    val event = buildEvent(
      queryString, body, path, userAgent, refererUri, hostname, ipAddress, request, nuid, ct)
    val sinkResponses = sinkEvent(event, partitionKey)

    val headers = getCookieHeader(config.cookieConfig, nuid) ++ List(
      RawHeader("P3P", "policyref=\"%s\", CP=\"%s\"".format(config.p3p.policyRef, config.p3p.CP)),
      getAccessControlAllowOriginHeader(request),
      `Access-Control-Allow-Credentials`(true)
    )

    val (httpResponse, badRedirectResponses) =
      buildHttpResponse(event, partitionKey, queryString, path, headers.toList, pixelExpected)
    (httpResponse, badRedirectResponses ++ sinkResponses)
  }

  /**
   * Creates a response to the CORS preflight Options request
   * @param request Incoming preflight Options request
   * @return Response granting permissions to make the actual request
   */
  override def preflightResponse(request: HttpRequest): HttpResponse =
    HttpResponse()
      .withHeaders(List(
        getAccessControlAllowOriginHeader(request),
        `Access-Control-Allow-Credentials`(true),
        `Access-Control-Allow-Headers`("Content-Type")
      ))

  /** Creates a response with a cross domain policiy file */
  override def flashCrossDomainPolicy: HttpResponse = HttpResponse(
    entity = HttpEntity(
      contentType = ContentType(MediaTypes.`text/xml`, HttpCharsets.`ISO-8859-1`),
      string = "<?xml version=\"1.0\"?>\n<cross-domain-policy>\n  <allow-access-from domain=\"*\" secure=\"false\" />\n</cross-domain-policy>"
    )
  )

  /** Builds a raw event from an Http request. */
  def buildEvent(
    queryString: Option[String],
    body: Option[String],
    path: String,
    userAgent: Option[String],
    refererUri: Option[String],
    hostname: String,
    ipAddress: String,
    request: HttpRequest,
    networkUserId: String,
    contentType: Option[String]
  ): CollectorPayload = {
    val e = new CollectorPayload(
      "iglu:com.snowplowanalytics.snowplow/CollectorPayload/thrift/1-0-0",
      ipAddress,
      System.currentTimeMillis,
      "UTF-8",
      collector
    )
    e.querystring = queryString.orNull
    body.foreach(e.body = _)
    e.path = path
    userAgent.foreach(e.userAgent = _)
    refererUri.foreach(e.refererUri = _)
    e.hostname = hostname
    e.networkUserId = networkUserId
    e.headers = (getHeaders(request) ++ contentType).asJava
    contentType.foreach(e.contentType = _)
    e
  }

  /** Produces the event to the configured sink. */
  def sinkEvent(
    event: CollectorPayload,
    partitionKey: String
  ): List[Array[Byte]] =
    sinks.good.getType match {
      case Kinesis if KinesisSink.shuttingDown =>
        logger.warn(s"Kinesis sink shutting down, cannot send event $event")
        List.empty
      case _ =>
        // Split events into Good and Bad
        val eventSplit = SplitBatch.splitAndSerializePayload(event, sinks.good.MaxBytes)
        // Send events to respective sinks
        val sinkResponseGood = sinks.good.storeRawEvents(eventSplit.good, partitionKey)
        val sinkResponseBad  = sinks.bad.storeRawEvents(eventSplit.bad, partitionKey)
        // Sink Responses for Test Sink
        sinkResponseGood ++ sinkResponseBad
    }

  /** Builds the final http response from  */
  def buildHttpResponse(
    event: CollectorPayload,
    partitionKey: String,
    queryString: String,
    path: String,
    headers: List[HttpHeader],
    pixelExpected: Boolean
  ): (HttpResponse, List[Array[Byte]]) =
    if (path.startsWith("/r/")) {
      val (r, l) = buildRedirectHttpResponse(event, partitionKey, queryString)
      (r.withHeaders(r.headers ++ headers), l)
    } else if (sinks.good.getType == Kinesis && KinesisSink.shuttingDown) {
      logger.warn(s"Kinesis sink shutting down, cannot process request")
      // So that the tracker knows the request failed and can try to resend later
      (HttpResponse(404, entity = "404 not found"), Nil)
    } else ((if (pixelExpected) {
      HttpResponse(entity = HttpEntity(
        contentType = ContentType(MediaTypes.`image/gif`),
        bytes = CollectorService.pixel))
    } else {
      // See https://github.com/snowplow/snowplow-javascript-tracker/issues/482
      HttpResponse(entity = "ok")
    }).withHeaders(headers), Nil)

  /** Builds the appropriate http response when dealing with click redirects. */
  def buildRedirectHttpResponse(
    event: CollectorPayload,
    partitionKey: String,
    queryString: String
  ): (HttpResponse, List[Array[Byte]]) = {
    val serializer = SplitBatch.ThriftSerializer.get()
    Uri.Query(queryString).toMap.get("u") match {
      case Some(target) => (HttpResponse(302).withHeaders(`Location`(target)), Nil)
      case None =>
        val serialized = new String(serializer.serialize(event))
        val badRow = createBadRow(event, "Redirect failed due to lack of u parameter", serializer)
        (HttpResponse(StatusCodes.BadRequest),
          sinks.bad.storeRawEvents(List(badRow), partitionKey))
    }
  }

  /**
   * Builds a cookie header with the network user id as value.
   * @param cookieConfig cookie configuration extracted from the collector configuration
   * @param networkUserId value of the cookie
   * @return the build cookie wrapped in a header
   */
  def getCookieHeader(
    cookieConfig: Option[CookieConfig],
    networkUserId: String
  ): Option[HttpHeader] =
    cookieConfig.map { config =>
      val responseCookie = HttpCookie(
        name    = config.name,
        value   = networkUserId,
        expires = Some(DateTime.now + config.expiration.toMillis),
        domain  = config.domain,
        path    = Some("/")
      )
      `Set-Cookie`(responseCookie)
    }

  /** Retrieves all headers from the request except Remote-Address and Raw-Requet-URI */
  def getHeaders(request: HttpRequest): Seq[String] = request.headers.flatMap {
    case _: `Remote-Address` | _: `Raw-Request-URI` => None
    case other => Some(other.toString)
  }

  /**
   * Gets the IP from a RemoteAddress. If ipAsPartitionKey is false, a UUID will be generated.
   * @param remoteAddress Address extracted from an HTTP request
   * @param ipPartitionKey Whether to use the ip as a partition key or a random UUID
   * @return a tuple of ip (unknown if it couldn't be extracted) and partition key
   */
  def getIpAndPartitionKey(
    remoteAddress: RemoteAddress, ipAsPartitionKey: Boolean
  ): (String, String) =
    remoteAddress.toOption.map(_.getHostAddress) match {
      case None     => ("unknown", UUID.randomUUID.toString)
      case Some(ip) => (ip, if (ipAsPartitionKey) ip else UUID.randomUUID.toString)
    }

  /**
   * Gets the network user id from the query string or the request cookie. If neither of those are
   * present, the result is a random UUID.
   * @param request Http request made
   * @param requestCookie cookie associated to the Http request
   * @return a network user id
   */
  def getNetworkUserId(request: HttpRequest, requestCookie: Option[HttpCookie]): String =
    request.uri.query().get("nuid")
      .orElse(requestCookie.map(_.value))
      .getOrElse(UUID.randomUUID.toString)

  /**
   * Creates an Access-Control-Allow-Origin header which specifically allows the domain which made
   * the request
   * @param request Incoming request
   * @return Header allowing only the domain which made the request or everything
   */
  def getAccessControlAllowOriginHeader(request: HttpRequest): HttpHeader =
    `Access-Control-Allow-Origin`(request.headers.find(_ match {
      case `Origin`(_) => true
      case _ => false
    }) match {
      case Some(`Origin`(origin)) => HttpOriginRange.Default(origin)
      case _ => HttpOriginRange.`*`
    })

  /** Puts together a bad row ready for sinking */
  private def createBadRow(
      event: CollectorPayload, message: String, serializer: TSerializer): Array[Byte] =
    BadRow(new String(serializer.serialize(event)), NonEmptyList(message))
      .toCompactJson
      .getBytes("UTF-8")
}
