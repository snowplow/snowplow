/*
 * Copyright (c) 2012-2014 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.enrich.common
package loaders

// Scalaz
import scalaz._
import Scalaz._

// This project
import utils.ConversionUtils

/**
 * The dedicated loader for events collected by
 * the Clojure Collector running on Tomcat. The
 * format started as an approximation of the
 * CloudFront format, but has now diverged as
 * we add support for POST payloads.
 */
object CljTomcatLoader extends Loader[String] {

  // The encoding used on these logs
  private val CollectorEncoding = "UTF-8"

  // The name of this collector
  private val CollectorName = "clj-tomcat"

  // Define the regular expression for extracting the fields
  // Adapted and evolved from the Clojure Collector's
  // regular expression
  private val CljTomcatRegex = {
    val w = "[\\s]+"   // Whitespace regex
    val ow = "(?:" + w // Non-capturing optional whitespace begins
    
    // Our regex follows. Try debuggex.com if it doesn't make sense
    (    "^([\\S]+)"     +   // Date          / date
    w  +  "([\\S]+)"     +   // Time          / time
    w  +  "(-)"          +   // -             / x-edge-location    added for consistency with CloudFront
    w  +  "([\\S]+)"     +   // BytesSent     / sc-bytes
    w  +  "([\\S]+)"     +   // IPAddress     / c-ip
    w  +  "([\\S]+)"     +   // Operation     / cs-method
    w  +  "([\\S]+)"     +   // Domain        / cs(Host)
    w  +  "([\\S]+)"     +   // Object        / cs-uri-stem
    w  +  "([\\S]+)"     +   // HttpStatus    / sc-status
    w  +  "([\\S]+)"     +   // Referer       / cs(Referer)
    w  +  "([\\S]+)"     +   // UserAgent     / cs(User Agent)
    w  +  "([\\S]+)"     +   // Querystring   / cs-uri-query
    ow +  "-"            +   // -             / cs(Cookie)         added for consistency with CloudFront
    w  +  "-"            +   // -             / x-edge-result-type added for consistency with CloudFront
    w  +  "-)?"          +   // -             / x-edge-request-id  added for consistency with CloudFront
    ow +  "([\\S]+)?"    +   // ContentType   /                    POST support
    w  +  "([\\S]+)?)?$").r  // PostBody      /                    POST support
  }

  // To extract the API vendor and version from the
  // the path to the requested object.
  // TODO: move this to somewhere not specific to
  // this collector
  private val ApiPathRegex = """^[\/]?([^\/]+)\/([^\/]+)[\/]?$""".r

  /**
   * Converts the source string into a 
   * ValidatedMaybeCollectorPayload.
   *
   * @param line A line of data to convert
   * @return either a set of validation
   *         errors or an Option-boxed
   *         CanonicalInput object, wrapped
   *         in a Scalaz ValidatioNel.
   */
  def toCollectorPayload(line: String): ValidatedMaybeCollectorPayload = {

    def build(qs: String, date: String, time: String, ip: String, ua: String, refr: String, api: Option[CollectorApi], ct: Option[String], bdy: Option[String]): ValidatedMaybeCollectorPayload = {
      val querystring = parseQuerystring(CloudfrontLoader.toOption(qs), CollectorEncoding)
      val timestamp = CloudfrontLoader.toTimestamp(date, time)
      val contentType = (for {
        enc <- ct
        raw  = ConversionUtils.decodeString(CollectorEncoding, "Content type", enc)
      } yield raw).sequenceU
      val body = (for {
        b64 <- bdy
        raw  = ConversionUtils.decodeBase64Url("Body", b64)
      } yield raw).sequenceU

      (timestamp.toValidationNel |@| querystring.toValidationNel |@| contentType.toValidationNel |@| body.toValidationNel) { (t, q, c, b) =>
        CollectorPayload(
          q,
          CollectorName,
          CollectorEncoding,
          None, // No hostname for CljTomcat
          t,
          CloudfrontLoader.toOption(ip),
          CloudfrontLoader.toOption(ua),
          CloudfrontLoader.toOption(refr),
          Nil,  // No headers for CljTomcat
          None, // No collector-set user ID for CljTomcat
          api,  // We may have API vendor and version
          c,    // We may have content type
          b     // We may have request body
        ).some
      }
    }

    line match {
      // A: For a GET request to /i, CljTomcat collector <= v0.6.0

      // A.1 Not a request for /i, drop the row silently
      case CljTomcatRegex(_, _, _, _, _, _, _, objct, _, _, _, _, null, null) if !isIceRequest(objct) =>
        None.success

      // A.2 Not a GET request for /i
      case CljTomcatRegex(_, _, _, _, _, op, _, _, _, _, _, _, null, null) if op.toUpperCase != "GET" =>
        s"Operation must be GET, not ${op.toUpperCase}, if request content type and body are not provided".failNel[Option[CollectorPayload]]

      // A.3 GET request for /i as expected
      case CljTomcatRegex(date, time, _, _, ip, _, _, _, _, refr, ua, qs, null, null) =>
        build(qs, date, time, ip, ua, refr, None, None, None) // API, content type and request body all unavailable

      // B: For a GET request to /i, CljTomcat collector >= v0.7.0

      // B.1 Not a request for /i, drop the row silently
      case CljTomcatRegex(_, _, _, _, _, _, _, objct, _, _, _, _, "-", "-") if !isIceRequest(objct) =>
        None.success

      // B.2 Not a GET request for /i
      case CljTomcatRegex(_, _, _, _, _, op, _, _, _, _, _, _, "-", "_") if op.toUpperCase != "GET" =>
        s"Operation must be GET, not ${op.toUpperCase}, if request content type and body are not provided".failNel[Option[CollectorPayload]]

      // B.3 GET request for /i as expected
      case CljTomcatRegex(date, time, _, _, ip, _, _, _, _, refr, ua, qs, "-", "-") =>
        build(qs, date, time, ip, ua, refr, None, None, None) // API, content type and request body all unavailable      

      // C: For a POST request with content type and body to /<api vendor>/<api version>, CljTomcat collector >= v0.7.0

      // C.1 Not a POST request
      case CljTomcatRegex(_, _, _, _, _, op, _, _, _, _, _, _, _, _) if op.toUpperCase != "POST" =>
        s"Operation must be POST, not ${op.toUpperCase}, if request content type and body are provided".failNel[Option[CollectorPayload]]

      // C.2 A POST, let's check we can discern API format
      case CljTomcatRegex(date, time, _, _, ip, _, _, objct, _, refr, ua, qs, ct, bdy) => objct match {
        case ApiPathRegex(vnd, ver) => build(qs, date, time, ip, ua, refr, CollectorApi(vnd, ver).some, ct.some, bdy.some)
        case _                      => s"Requested object ${objct} does not match (/)vendor/version(/) pattern".failNel[Option[CollectorPayload]]
      }

      // D. Row not recognised
      case _ =>
        "Line does not match raw event format for Clojure Collector".failNel[Option[CollectorPayload]]
    }
  }
}
