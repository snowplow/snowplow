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
    val ow = "(?:" + w // Optional whitespace begins
    
    // Our regex follows
    (   "([\\S]+)"  +   // Date          / date
    w + "([\\S]+)"  +   // Time          / time
    w + "(-)"       +   // -             / x-edge-location    added for consistency with CloudFront
    w + "([\\S]+)"  +   // BytesSent     / sc-bytes
    w + "([\\S]+)"  +   // IPAddress     / c-ip
    w + "([\\S]+)"  +   // Operation     / cs-method
    w + "([\\S]+)"  +   // Domain        / cs(Host)
    w + "([\\S]+)"  +   // Object        / cs-uri-stem
    w + "([\\S]+)"  +   // HttpStatus    / sc-status
    w + "([\\S]+)"  +   // Referer       / cs(Referer)
    w + "([\\S]+)"  +   // UserAgent     / cs(User Agent)
    w + "([\\S]+)"  +   // Querystring   / cs-uri-query
    ow + "-"        +   // -             / cs(Cookie)         added for consistency with CloudFront
    w +  "-"        +   // -             / x-edge-result-type added for consistency with CloudFront
    w +  "-)?"      +   // -             / x-edge-request-id  added for consistency with CloudFront
    ow + "[\\S]+)?" +   // ContentType   /                    POST support
    ow + "[\\S]+)?").r  // PostBody      /                    POST support
  }

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
  def toCollectorPayload(line: String): ValidatedMaybeCollectorPayload = line match {
    
    // A: Expecting a GET request to /i

    // A.1 Not a request for /i
    case CljTomcatRegex(_, _, _, _, _, _, _, objct, _, _, _, _) if !isIceRequest(objct) =>
      None.success

    // A.2 Not a GET request for /i
    case CljTomcatRegex(_, _, _, _, _, op, _, _, _, _, _, _) if op.toUpperCase != "GET" =>
      s"Operation must be GET, not ${op.toUpperCase}, for Clojure Collector if request content type and body not provided".failNel[Option[CollectorPayload]]

    // A.3 GET request for /i as expected
    case CljTomcatRegex(date,
                 time,
                 _,
                 _,
                 ip,
                 _,
                 _,
                 objct,
                 _,
                 refr,
                 ua,
                 qs/*,
                 _,
                 ct,
                 body*/) => {

      // Validations
      val timestamp = CloudfrontLoader.toTimestamp(date, time)
      val querystring = parseQuerystring(CloudfrontLoader.toOption(qs), CollectorEncoding)

      (timestamp.toValidationNel |@| querystring.toValidationNel) { (t, q) =>
        CollectorPayload(
          q,
          CollectorName,
          CollectorEncoding,
          None, // No hostname for CloudFront
          t,
          CloudfrontLoader.toOption(ip),
          CloudfrontLoader.toOption(ua),
          CloudfrontLoader.toOption(refr),
          Nil,  // No headers for CloudFront
          None  // No collector-set user ID for CloudFront
        ).some
      }
    }

    // B: Expecting a POST request to <api vendor>/<api version>
    // TODO

    // C. Row not recognised
    case _ => "Line does not match raw event format for Clojure Collector".failNel[Option[CollectorPayload]]
  }
}
