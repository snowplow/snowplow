/*
 * Copyright (c) 2013-2019 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.collectors.scalastream

import java.util.UUID

import scala.collection.JavaConverters._

import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers._
import akka.http.scaladsl.model.headers.CacheDirectives._

import com.snowplowanalytics.snowplow.CollectorPayload.thrift.model1.CollectorPayload
import org.apache.commons.codec.binary.Base64
import org.slf4j.LoggerFactory

import generated.BuildInfo
import model._
import utils.SplitBatch

/**
 * Service responding to HTTP requests, mainly setting a cookie identifying the user and storing
 * events
 */
trait Service {
  def preflightResponse(req: HttpRequest): HttpResponse
  def flashCrossDomainPolicy: HttpResponse
  def rootResponse: HttpResponse
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
    doNotTrack: Boolean,
    contentType: Option[ContentType] = None
  ): (HttpResponse, List[Array[Byte]])
  def cookieName: Option[String]
  def doNotTrackCookie: Option[DntCookieMatcher]
  def determinePath(vendor: String, version: String): String
  def enableDefaultRedirect: Boolean
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

  private val collector = s"${BuildInfo.shortName}-${BuildInfo.version}-" +
    config.streams.sink.getClass.getSimpleName.toLowerCase

  override val cookieName = config.cookieName
  override val doNotTrackCookie = config.doNotTrackHttpCookie
  override val enableDefaultRedirect = config.enableDefaultRedirect

  /**
   * Determines the path to be used in the response,
   * based on whether a mapping can be found in the config for the original request path.
   */
  override def determinePath(vendor: String, version: String): String = {
    val original = s"/$vendor/$version"
    config.paths.getOrElse(original, original)
  }

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
    doNotTrack: Boolean,
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
    val sinkResponses = if (!bounce && !doNotTrack) sinkEvent(event, partitionKey) else Nil

    val headers = bounceLocationHeader(
      queryParams,
      request,
      config.cookieBounce,
      bounce) ++
      cookieHeader(request, config.cookieConfig, nuid, doNotTrack) ++
      cacheControl(pixelExpected) ++
      List(
        RawHeader("P3P", "policyref=\"%s\", CP=\"%s\"".format(config.p3p.policyRef, config.p3p.CP)),
        accessControlAllowOriginHeader(request),
        `Access-Control-Allow-Credentials`(true)
      )

    val (httpResponse, badRedirectResponses) = buildHttpResponse(
      event, queryParams, headers.toList, redirect, pixelExpected, bounce, config.redirectMacro)
    (httpResponse, badRedirectResponses ++ sinkResponses)
  }

  /**
   * Creates a response to the CORS preflight Options request
   * @param request Incoming preflight Options request
   * @return Response granting permissions to make the actual request
   */
  override def preflightResponse(request: HttpRequest): HttpResponse =
    preflightResponse(request, config.cors)

  def preflightResponse(request: HttpRequest, corsConfig: CORSConfig): HttpResponse =
    HttpResponse()
      .withHeaders(List(
        accessControlAllowOriginHeader(request),
        `Access-Control-Allow-Credentials`(true),
        `Access-Control-Allow-Headers`("Content-Type"),
        `Access-Control-Max-Age`(corsConfig.accessControlMaxAge.toSeconds)
      ))

  override def flashCrossDomainPolicy: HttpResponse =
    flashCrossDomainPolicy(config.crossDomain)

  /** Creates a response with a cross domain policiy file */
  def flashCrossDomainPolicy(config: CrossDomainConfig): HttpResponse =
    if (config.enabled) {
      HttpResponse(entity = HttpEntity(
        contentType = ContentType(MediaTypes.`text/xml`, HttpCharsets.`ISO-8859-1`),
        string = """<?xml version="1.0"?>""" + "\n<cross-domain-policy>\n" +
          config.domains.map(d => s"""  <allow-access-from domain=\"$d\" secure=\"${config.secure}\" />""").mkString("\n") +
          "\n</cross-domain-policy>"
      ))
    } else {
      HttpResponse(404, entity = "404 not found")
    }


  override def rootResponse: HttpResponse =
    rootResponse(config.rootResponse)

  def rootResponse(c: RootResponseConfig): HttpResponse =
    if (c.enabled) {
      val rawHeaders = c.headers.map { case (k, v) => RawHeader(k, v) }.toList
      HttpResponse(c.statusCode, rawHeaders, HttpEntity(c.body))
    } else {
      HttpResponse(404, entity = "404 not found")
    }

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
  ): List[Array[Byte]] = {
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
    queryParams: Map[String, String],
    headers: List[HttpHeader],
    redirect: Boolean,
    pixelExpected: Boolean,
    bounce: Boolean,
    redirectMacroConfig: RedirectMacroConfig
  ): (HttpResponse, List[Array[Byte]]) =
    if (redirect) {
      val (r, l) = buildRedirectHttpResponse(event, queryParams, redirectMacroConfig)
      (r.withHeaders(r.headers ++ headers), l)
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
    queryParams: Map[String, String],
    redirectMacroConfig: RedirectMacroConfig
  ): (HttpResponse, List[Array[Byte]]) =
    queryParams.get("u") match {
      case Some(target) =>
        val canReplace = redirectMacroConfig.enabled && event.isSetNetworkUserId
        val token = redirectMacroConfig.placeholder.getOrElse(s"$${SP_NUID}")
        val replacedTarget =
          if (canReplace) target.replaceAllLiterally(token, event.networkUserId)
          else target
        (HttpResponse(StatusCodes.Found).withHeaders(`RawHeader`("Location", replacedTarget)), Nil)
      case None => (HttpResponse(StatusCodes.BadRequest), Nil)
    }

  /**
   * Builds a cookie header with the network user id as value.
   * @param cookieConfig cookie configuration extracted from the collector configuration
   * @param networkUserId value of the cookie
   * @param doNotTrack whether do not track is enabled or not
   * @return the build cookie wrapped in a header
   */
  def cookieHeader(
    request: HttpRequest,
    cookieConfig: Option[CookieConfig],
    networkUserId: String,
    doNotTrack: Boolean
  ): Option[HttpHeader] =
    if (doNotTrack) {
      None
    } else {
      cookieConfig.map { config =>
        val responseCookie = HttpCookie(
          name    = config.name,
          value   = networkUserId,
          expires = Some(DateTime.now + config.expiration.toMillis),
          domain  = cookieDomain(request.headers, config.domains, config.fallbackDomain),
          path    = Some("/"),
          secure    = config.secure,
          httpOnly  = config.httpOnly,
          extension = config.sameSite.map(value => s"SameSite=$value")
        )
        `Set-Cookie`(responseCookie)
      }
    }

  /** Build a location header redirecting to itself to check if third-party cookies are blocked. */
  def bounceLocationHeader(
    queryParams: Map[String, String],
    request: HttpRequest,
    bounceConfig: CookieBounceConfig,
    bounce: Boolean
  ): Option[HttpHeader] =
    if (bounce) {
      val forwardedScheme = for {
        headerName <- bounceConfig.forwardedProtocolHeader
        headerValue <- request.headers
          .find(_.lowercaseName == headerName.toLowerCase)
          .map(_.value().toLowerCase())
        scheme <-
          if (Set("http", "https").contains(headerValue)) {
            Some(headerValue)
          } else {
            logger.warn(s"Header $headerName contains invalid protocol value $headerValue.")
            None
          }
      } yield scheme

      val redirectUri = request.uri
        .withQuery(Uri.Query(queryParams + (bounceConfig.name -> "true")))
        .withScheme(forwardedScheme.getOrElse(request.uri.scheme))

      Some(`Location`(redirectUri))
    } else {
      None
    }

  /** Retrieves all headers from the request except Remote-Address and Raw-Request-URI */
  def headers(request: HttpRequest): Seq[String] = request.headers.flatMap {
    case _: `Remote-Address` | _: `Raw-Request-URI` => None
    case other => Some(other.toString)
  }

  /** If the pixel is requested, this attaches cache control headers to the response to prevent any caching. */
  def cacheControl(pixelExpected: Boolean): List[`Cache-Control`] =
    if (pixelExpected) List(`Cache-Control`(`no-cache`, `no-store`, `must-revalidate`))
    else Nil

  /**
   * Determines the cookie domain to be used by inspecting the Origin header of the request
   * and trying to find a match in the list of domains specified in the config file.
   * @param headers The headers from the http request.
   * @param domains The list of cookie domains from the configuration.
   * @param fallbackDomain The fallback domain from the configuration.
   * @return The domain to be sent back in the response, unless no cookie domains are configured.
   * The Origin header may include multiple domains. The first matching domain is returned.
   * If no match is found, the fallback domain is used if configured. Otherwise, the cookie domain is not set.
   */
  def cookieDomain(headers: Seq[HttpHeader], domains: Option[List[String]], fallbackDomain: Option[String]): Option[String] =
    (for {
      domainList <- domains
      origins <- headers.collectFirst { case header: `Origin` => header.origins }
      originHosts = extractHosts(origins)
      domainToUse <- domainList.find(domain => originHosts.exists(validMatch(_, domain)))
    } yield domainToUse).orElse(fallbackDomain)

  /** Extracts the host names from a list of values in the request's Origin header. */
  def extractHosts(origins: Seq[HttpOrigin]): Seq[String] =
    origins.map(origin => origin.host.host.address())

  /**
   * Ensures a match is valid.
   * We only want matches where:
   * a.) the Origin host is exactly equal to the cookie domain from the config
   * b.) the Origin host is a subdomain of the cookie domain from the config.
   * But we want to avoid cases where the cookie domain from the config is randomly
   * a substring of the Origin host, without any connection between them.
   */
  def validMatch(host: String, domain: String): Boolean =
    host == domain || host.endsWith("." + domain)

  /**
   * Gets the IP from a RemoteAddress. If ipAsPartitionKey is false, a UUID will be generated.
   * @param remoteAddress Address extracted from an HTTP request
   * @param ipAsPartitionKey Whether to use the ip as a partition key or a random UUID
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
}
