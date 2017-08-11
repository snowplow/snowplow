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
    val queryParams = Uri.Query(queryString).toMap

    val (ipAddress, partitionKey) = ipAndPartitionKey(ip, config.streams.useIpAddressAsPartitionKey)

    val redirect = path.startsWith("/r/")

    val nuidOpt = networkUserId(request, cookie)
    val bouncing = queryParams.get(config.cookieBounce.name).isDefined
    // we bounce if it's enabled and we couldn't retrieve the nuid and we're not already bouncing
    val bounce = config.cookieBounce.enabled && nuidOpt.isEmpty && !bouncing &&
      pixelExpected && !redirect
    val nuid = nuidOpt.getOrElse {
      if (bouncing) config.cookieBounce.fallbackNetworkUserId
      else UUID.randomUUID().toString
    }

    val ct = contentType.map(_.value.toLowerCase)
    val event = buildEvent(
      queryString, body, path, userAgent, refererUri, hostname, ipAddress, request, nuid, ct)
    // we don't store events in case we're bouncing
    val sinkResponses = if (!bounce) sinkEvent(event, partitionKey) else Nil

    val headers = bounceLocationHeader(queryParams, request.uri, config.cookieBounce.name, bounce) ++
      cookieHeader(config.cookieConfig, nuid) ++ List(
        RawHeader("P3P", "policyref=\"%s\", CP=\"%s\"".format(config.p3p.policyRef, config.p3p.CP)),
        accessControlAllowOriginHeader(request),
        `Access-Control-Allow-Credentials`(true)
      )

    val (httpResponse, badRedirectResponses) = buildHttpResponse(
      event, partitionKey, queryParams, headers.toList, redirect, pixelExpected, bounce)
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
        accessControlAllowOriginHeader(request),
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
    e.headers = (headers(request) ++ contentType).asJava
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
    queryParams: Map[String, String],
    headers: List[HttpHeader],
    redirect: Boolean,
    pixelExpected: Boolean,
    bounce: Boolean
  ): (HttpResponse, List[Array[Byte]]) =
    if (redirect) {
      val (r, l) = buildRedirectHttpResponse(event, partitionKey, queryParams)
      (r.withHeaders(r.headers ++ headers), l)
    } else if (sinks.good.getType == Kinesis && KinesisSink.shuttingDown) {
      logger.warn(s"Kinesis sink shutting down, cannot process request")
      // So that the tracker knows the request failed and can try to resend later
      (HttpResponse(StatusCodes.NotFound, entity = "404 not found"), Nil)
    } else {
      (buildUsualHttpResponse(pixelExpected, bounce).withHeaders(headers), Nil)
    }

  /** Builds the appropriate http response when not dealing with click redirects. */
  def buildUsualHttpResponse(pixelExpected: Boolean, bounce: Boolean): HttpResponse =
    (pixelExpected, bounce) match {
      case (true, true)  => HttpResponse(StatusCodes.Found)
      case (true, false) => HttpResponse(entity = HttpEntity(
        contentType = ContentType(MediaTypes.`image/gif`),
        bytes = CollectorService.pixel))
      // See https://github.com/snowplow/snowplow-javascript-tracker/issues/482
      case _             => HttpResponse(entity = "ok")
    }

  /** Builds the appropriate http response when dealing with click redirects. */
  def buildRedirectHttpResponse(
    event: CollectorPayload,
    partitionKey: String,
    queryParams: Map[String, String]
  ): (HttpResponse, List[Array[Byte]]) =
    queryParams.get("u") match {
      case Some(target) => (HttpResponse(StatusCodes.Found).withHeaders(`Location`(target)), Nil)
      case None =>
        val badRow = createBadRow(event, "Redirect failed due to lack of u parameter")
        (HttpResponse(StatusCodes.BadRequest),
          sinks.bad.storeRawEvents(List(badRow), partitionKey))
    }

  /**
   * Builds a cookie header with the network user id as value.
   * @param cookieConfig cookie configuration extracted from the collector configuration
   * @param networkUserId value of the cookie
   * @return the build cookie wrapped in a header
   */
  def cookieHeader(
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

  /** Build a location header redirecting to itself to check if third-party cookies are blocked. */
  def bounceLocationHeader(
    queryParams: Map[String, String],
    uri: Uri,
    cookieBounceName: String,
    bounce: Boolean
  ): Option[HttpHeader] =
    if (bounce) {
      val redirectUri = uri.withQuery(Uri.Query(queryParams + (cookieBounceName -> "true")))
      Some(`Location`(redirectUri))
    } else {
      None
    }

  /** Retrieves all headers from the request except Remote-Address and Raw-Requet-URI */
  def headers(request: HttpRequest): Seq[String] = request.headers.flatMap {
    case _: `Remote-Address` | _: `Raw-Request-URI` => None
    case other => Some(other.toString)
  }

  /**
   * Gets the IP from a RemoteAddress. If ipAsPartitionKey is false, a UUID will be generated.
   * @param remoteAddress Address extracted from an HTTP request
   * @param ipPartitionKey Whether to use the ip as a partition key or a random UUID
   * @return a tuple of ip (unknown if it couldn't be extracted) and partition key
   */
  def ipAndPartitionKey(
    remoteAddress: RemoteAddress, ipAsPartitionKey: Boolean
  ): (String, String) =
    remoteAddress.toOption.map(_.getHostAddress) match {
      case None     => ("unknown", UUID.randomUUID.toString)
      case Some(ip) => (ip, if (ipAsPartitionKey) ip else UUID.randomUUID.toString)
    }

  /**
   * Gets the network user id from the query string or the request cookie.
   * @param request Http request made
   * @param requestCookie cookie associated to the Http request
   * @return a network user id
   */
  def networkUserId(request: HttpRequest, requestCookie: Option[HttpCookie]): Option[String] =
    request.uri.query().get("nuid")
      .orElse(requestCookie.map(_.value))

  /**
   * Creates an Access-Control-Allow-Origin header which specifically allows the domain which made
   * the request
   * @param request Incoming request
   * @return Header allowing only the domain which made the request or everything
   */
  def accessControlAllowOriginHeader(request: HttpRequest): HttpHeader =
    `Access-Control-Allow-Origin`(request.headers.find {
      case `Origin`(_) => true
      case _ => false
    } match {
      case Some(`Origin`(origin)) => HttpOriginRange.Default(origin)
      case _ => HttpOriginRange.`*`
    })

  /** Puts together a bad row ready for sinking */
  private def createBadRow(event: CollectorPayload, message: String): Array[Byte] =
    BadRow(new String(SplitBatch.ThriftSerializer.get().serialize(event)), NonEmptyList(message))
      .toCompactJson
      .getBytes("UTF-8")
}
