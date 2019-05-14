/*
 * Copyright (c) 2012-2019 Snowplow Analytics Ltd. All rights reserved.
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

import java.nio.charset.StandardCharsets.UTF_8

import cats.data.ValidatedNel
import cats.implicits._

import utils.ConversionUtils

/**
 * The dedicated loader for events collected by the Clojure Collector running on Tomcat. The
 * format started as an approximation of the CloudFront format, but has now diverged as
 * we add support for POST payloads.
 */
object CljTomcatLoader extends Loader[String] {
  // The encoding used on these logs
  private val CollectorEncoding = UTF_8

  // The name of this collector
  private val CollectorName = "clj-tomcat"

  // Define the regular expression for extracting the fields
  // Adapted and evolved from the Clojure Collector's regular expression
  private val CljTomcatRegex = {
    val w = "[\\s]+" // Whitespace regex
    val ow = "(?:" + w // Non-capturing optional whitespace begins

    // Our regex follows. Try debuggex.com if it doesn't make sense
    ("^([\\S]+)" + // Date          / date
      w + "([\\S]+)" + // Time          / time
      w + "(-)" + // -             / x-edge-location    added for consistency with CloudFront
      w + "([\\S]+)" + // BytesSent     / sc-bytes
      w + "([\\S]+)" + // IPAddress     / c-ip
      w + "([\\S]+)" + // Operation     / cs-method
      w + "([\\S]+)" + // Domain        / cs(Host)
      w + "([\\S]+)" + // Object        / cs-uri-stem
      w + "([\\S]+)" + // HttpStatus    / sc-status
      w + "([\\S]+)" + // Referer       / cs(Referer)
      w + "([\\S]+)" + // UserAgent     / cs(User Agent)
      w + "([\\S]+)" + // Querystring   / cs-uri-query
      ow + "-" + // -             / cs(Cookie)         added for consistency with CloudFront
      w + "-" + // -             / x-edge-result-type added for consistency with CloudFront
      w + "-)?" + // -             / x-edge-request-id  added for consistency with CloudFront
      ow + "([\\S]+)?" + // ContentType   /                    POST support
      w + "([\\S]+)?)?$").r // PostBody      /                    POST support
  }

  /**
   * Converts the source string into a ValidatedMaybeCollectorPayload.
   * @param line A line of data to convert
   * @return either a set of validation errors or an Option-boxed CanonicalInput object, wrapped
   * in a Scalaz ValidatioNel.
   */
  def toCollectorPayload(line: String): ValidatedNel[String, Option[CollectorPayload]] = {
    def build(
      qs: String,
      date: String,
      time: String,
      ip: String,
      ua: String,
      refr: String,
      objct: String,
      ct: Option[String],
      bdy: Option[String]
    ): ValidatedNel[String, Option[CollectorPayload]] = {
      val querystring = parseQuerystring(CloudfrontLoader.toOption(qs), CollectorEncoding)
      val timestamp = CloudfrontLoader.toTimestamp(date, time)
      val contentType = (for {
        enc <- ct
        raw = ConversionUtils.decodeString(CollectorEncoding, "Content type", enc)
      } yield raw).sequence
      val body = (for {
        b64 <- bdy
        raw = ConversionUtils.decodeBase64Url(b64) // body
      } yield raw).sequence
      val api = CollectorApi.parse(objct)

      (
        timestamp.toValidatedNel,
        querystring.toValidatedNel,
        api.toValidatedNel,
        contentType.toValidatedNel,
        body.toValidatedNel
      ).mapN { (t, q, a, c, b) =>
        CollectorPayload(
          q,
          CollectorName,
          CollectorEncoding.toString,
          None, // No hostname for CljTomcat
          Some(t),
          CloudfrontLoader.toOption(ip),
          CloudfrontLoader.toOption(ua),
          CloudfrontLoader.toOption(refr),
          Nil, // No headers for CljTomcat
          None, // No collector-set user ID for CljTomcat
          a, // API vendor and version
          c, // We may have content type
          b // We may have request body
        ).some
      }
    }

    line match {
      // A. For a request, to CljTomcat collector <= v0.6.0
      case CljTomcatRegex(date, time, _, _, ip, _, _, objct, _, refr, ua, qs, null, null) =>
        // API, content type and request body all unavailable
        build(qs, date, time, ip, ua, refr, objct, None, None)
      // B: For a request without body and potentially a content type, to CljTomcat collector >= v0.7.0

      // B.1 No body or content type
      // TODO: really we ought to be matching on "-", not-"-" and not-"-", "-" as well
      case CljTomcatRegex(date, time, _, _, ip, _, _, objct, _, refr, ua, qs, "-", "-") =>
        // API, content type and request body all unavailable
        build(qs, date, time, ip, ua, refr, objct, None, None)

      // B.2 No body but has content type
      case CljTomcatRegex(date, time, _, _, ip, _, _, objct, _, refr, ua, qs, ct, "-") =>
        // API and request body unavailable
        build(qs, date, time, ip, ua, refr, objct, ct.some, None)

      // C: For a request with content type and/or body, to CljTomcat collector >= v0.7.0

      // C.1 Not a POST request
      case CljTomcatRegex(_, _, _, _, _, op, _, _, _, _, _, _, _, _) if op.toUpperCase != "POST" =>
        (s"Operation must be POST, not ${op.toUpperCase}, if request content type and/or " +
          "body are provided").invalidNel

      // C.2 A POST, let's check we can discern API format
      // TODO: we should check for nulls/"-"s for ct and body below
      case CljTomcatRegex(date, time, _, _, ip, _, _, objct, _, refr, ua, qs, ct, bdy) =>
        build(qs, date, time, ip, ua, refr, objct, ct.some, bdy.some)

      // D. Row not recognised
      case _ =>
        "Line does not match raw event format for Clojure Collector".invalidNel
    }
  }
}
