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

// Apache Commons
import org.apache.commons.lang3.StringUtils

// Joda-Time
import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}

// This project
import utils.ConversionUtils.singleEncodePcts

/**
 * The dedicated loader for events
 * collected by CloudFront.
 *
 * We support the following CloudFront access
 * log formats:
 *
 * 1. Pre-12 Sep 2012
 * 2. 12 Sep 2012 - 21 Oct 2013
 * 3. 21 Oct 2013 - 29 Apr 2014
 * 4. Potential future updates, provided they
 *    are solely additive in nature
 *
 * For more details on this format, please see:
 * http://docs.amazonwebservices.com/AmazonCloudFront/latest/DeveloperGuide/AccessLogs.html#LogFileFormat
 */
object CloudfrontLoader extends Loader[String] {

  // The encoding used on CloudFront logs
  private val CollectorEncoding = "UTF-8"

  // The name of this collector
  private val CollectorName = "cloudfront"

  // Define the regular expression for extracting the fields
  // Adapted from Amazon's own cloudfront-loganalyzer.tgz
  private val CfRegex = {
    val w = "[\\s]+"   // Whitespace regex
    val ow = "(?:" + w // Optional whitespace begins
    
    // Our regex follows
    (   "([\\S]+)"  +   // Date          / date
    w + "([\\S]+)"  +   // Time          / time
    w + "([\\S]+)"  +   // EdgeLocation  / x-edge-location
    w + "([\\S]+)"  +   // BytesSent     / sc-bytes
    w + "([\\S]+)"  +   // IPAddress     / c-ip
    w + "([\\S]+)"  +   // Operation     / cs-method
    w + "([\\S]+)"  +   // Domain        / cs(Host)
    w + "([\\S]+)"  +   // Object        / cs-uri-stem
    w + "([\\S]+)"  +   // HttpStatus    / sc-status
    w + "([\\S]+)"  +   // Referer       / cs(Referer)
    w + "([\\S]+)"  +   // UserAgent     / cs(User Agent)
    w + "([\\S]+)"  +   // Querystring   / cs-uri-query
    ow + "[\\S]*"   +   // CookieHeader  / cs(Cookie)         added 12 Sep 2012 // TODO: why the *?
    w +  "[\\S]+"   +   // ResultType    / x-edge-result-type added 12 Sep 2012
    w +  "[\\S]+)?" +   // X-Amz-Cf-Id   / x-edge-request-id  added 12 Sep 2012
    ow + "[\\S]+"   +   // XHostHeader   / x-host-header      added 21 Oct 2013
    w +  "[\\S]+"   +   // CsProtocol    / cs-protocol        added 21 Oct 2013
    w +  "[\\S]+)?" +   // CsBytes       / cs-bytes           added 21 Oct 2013
    ow + "[\\S]+"   +   // TimeTaken     / time-taken         added 29 Apr 2014
    w +      ".*)?").r  // Anything added in the future by Amazon  
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
    
    // 1. Header row
    case h if (h.startsWith("#Version:") || h.startsWith("#Fields:")) =>
      None.success
    
    // 2. Not a GET request
    case CfRegex(_, _, _, _, _, op, _, _, _, _, _, _) if op.toUpperCase != "GET" =>
      s"Only GET operations supported for CloudFront Collector, not ${op.toUpperCase}".failNel[Option[CollectorPayload]]

    // 4. Row matches CloudFront format
    case CfRegex(date,
                 time,
                 _,
                 _,
                 ip,
                 _,
                 _,
                 objct,
                 _,
                 rfr,
                 ua,
                 qs) => {

      // Validations, and let's strip double-encodings
      val timestamp = toTimestamp(date, time)
      val querystring = {
        val q = toOption(singleEncodePcts(qs))
        parseQuerystring(q, CollectorEncoding)
      }

      // No validation (yet) on the below
      val userAgent  = singleEncodePcts(ua)
      val refr = singleEncodePcts(rfr)
      val referer = toOption(refr) map toCleanUri

      val api = CollectorApi.parse(objct)

      (timestamp.toValidationNel |@| querystring.toValidationNel |@| api.toValidationNel) { (t, q, a) =>
        CollectorPayload(
          q,
          CollectorName,
          CollectorEncoding,
          None, // No hostname for CloudFront
          Some(t),
          toOption(ip),
          toOption(userAgent),
          referer,
          Nil,  // No headers for CloudFront
          None, // No collector-set user ID for CloudFront
          a,    // API vendor/version
          None, // No content type
          None  // No request body
        ).some
      }
    }

    // 3. Row not recognised
    case _ => "Line does not match CloudFront header or data row formats".failNel[Option[CollectorPayload]]
  }

  /**
   * Converts a CloudFront log-format date and
   * a time to a timestamp.
   *
   * @param date The CloudFront log-format date
   * @param time The CloudFront log-format time
   * @return the timestamp as a Joda DateTime
   *         or an error String, all wrapped in
   *         a Scalaz Validation
   */
  def toTimestamp(date: String, time: String): Validation[String, DateTime] =
    try {
      DateTime.parse("%sT%s+00:00".format(date, time)).success // Construct a UTC ISO date from CloudFront date and time
    } catch {
      case e => "Unexpected exception converting date [%s] and time [%s] to timestamp: [%s]".format(date, time, e.getMessage).fail
    }

  /**
   * Checks whether a String field is a hyphen
   * "-", which is used by CloudFront to signal
   * a null.
   *
   * @param field The field to check
   * @return True if the String was a hyphen "-"
   */
  def toOption(field: String): Option[String] = Option(field) match {
    case Some("-") => None
    case Some("")  => None
    case s => s // Leaves any other Some(x) or None as-is
  }

  /**
   * 'Cleans' a string to make it parsable by
   * URLDecoder.decode.
   * 
   * The '%' character seems to be appended to the
   * end of some URLs in the CloudFront logs, causing
   * Exceptions when using URLDecoder.decode. Perhaps
   * a CloudFront bug?
   *
   * @param s The String to clean
   * @return the cleaned string
   */
  private[loaders] def toCleanUri(uri: String): String = 
    StringUtils.removeEnd(uri, "%")
}
