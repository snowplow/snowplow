/*
 * Copyright (c) 2012-2013 SnowPlow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.enrich.hadoop
package inputs

// Scalaz
import scalaz._
import Scalaz._

// Apache Commons
import org.apache.commons.lang.StringUtils

// Joda-Time
import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}

/**
 * The dedicated loader for events
 * collected by CloudFront.
 */
object CloudFrontLoader extends CloudFrontLikeLoader {

  /**
   * Returns the InputSource for this
   * loader.
   *
   * TODO: repetition of the identifier
   * String from getCollectorLoader. Can
   * we prevent duplication?
   */
  def getSource = InputSource("cloudfront", None)
}

/**
 * The dedicated loader for events
 * collected by the Clojure
 * Collector running on Tomcat (with
 * a Tomcat log format which
 * approximates the CloudFront format).
 */
object CljTomcatLoader extends CloudFrontLikeLoader {

  /**
   * Returns the InputSource for this
   * loader.
   *
   *
   * TODO: we need to update this in
   * this future when we have a way
   * of retrieving the Clojure Collector's
   * version (currently it's hardcoded
   * to clj-tomcat).
   *
   * TODO: repetition of the identifier
   * String from getCollectorLoader. Can
   * we prevent duplication?
   */

  def getSource = InputSource("clj-tomcat", None)
}

/**
 * Trait to hold helpers related to the
 * CloudFront input format.
 *
 * By "CloudFront input format", we mean the
 * CloudFront access log format for download
 * distributions (not streaming), September
 * 2012 release but with support for the pre-
 * September 2012 format as well.
 *
 * For more details on this format, please see:
 * http://docs.amazonwebservices.com/AmazonCloudFront/latest/DeveloperGuide/AccessLogs.html#LogFileFormat
 */
trait CloudFrontLikeLoader extends CollectorLoader {

  /**
   * Gets the source of this input.
   * Implemented by the implementing
   * objects (see above).
   *
   * TODO: we need to update this in
   * this future when we have a way
   * of retrieving the Clojure Collector's
   * version (currently it's hardcoded
   * to clj-tomcat).
   */
  def getSource: InputSource

  // The encoding used on CloudFront logs
  private val CfEncoding = "UTF-8"

  // Define the regular expression for extracting the fields
  // Adapted from Amazon's own cloudfront-loganalyzer.tgz
  private val CfRegex = {
    val w = "[\\s]+"   // Whitespace regex
    val ow = "(?:" + w // Optional whitespace begins
    
    // Our regex follows
    (   "([\\S]+)" +   // Date          / date
    w + "([\\S]+)" +   // Time          / time
    w + "([\\S]+)" +   // EdgeLocation  / x-edge-location
    w + "([\\S]+)" +   // BytesSent     / sc-bytes
    w + "([\\S]+)" +   // IPAddress     / c-ip
    w + "([\\S]+)" +   // Operation     / cs-method
    w + "([\\S]+)" +   // Domain        / cs(Host)
    w + "([\\S]+)" +   // Object        / cs-uri-stem
    w + "([\\S]+)" +   // HttpStatus    / sc-status
    w + "([\\S]+)" +   // Referer       / cs(Referer)
    w + "([\\S]+)" +   // UserAgent     / cs(User Agent)
    w + "([\\S]+)" +   // Querystring   / cs-uri-query
    ow + "[\\S]+"  +   // CookieHeader  / cs(Cookie)         added 12 Sep 2012
    w +  "[\\S]+"  +   // ResultType    / x-edge-result-type added 12 Sep 2012
    w +  "[\\S]+)?").r // X-Amz-Cf-Id   / x-edge-request-id  added 12 Sep 2012
  }

  /**
   * Converts the source string into a 
   * MaybeCanonicalInput.
   *
   * @param line A line of data to convert
   * @return either a set of validation
   *         errors or an Option-boxed
   *         CanonicalInput object, wrapped
   *         in a Scalaz ValidatioNel.
   */
  def toCanonicalInput(line: String): ValidatedMaybeCanonicalInput = line match {
    
    // 1. Header row
    case h if (h.startsWith("#Version:") ||
               h.startsWith("#Fields:"))    => None.success
    
    // 2. Row matches CloudFront format
    case CfRegex(date,
                 time,
                 _,
                 _,
                 ipAddress,
                 _,
                 _,
                 objct,
                 _,
                 referer,
                 userAgent,
                 querystring) => {

      // Is this a request for the tracker? Might be a browser favicon request or similar
      if (!isIceRequest(objct)) return None.success

      // Validations
      val timestamp = toTimestamp(date, time)
      val payload = toGetPayload(querystring)

      // No validation (yet) on the below
      val ip  = toOption(ipAddress)
      val ua  = toOption(userAgent)
      val rfr = toOption(referer) map toCleanUri

      (timestamp.toValidationNel |@| payload.toValidationNel) { (t, p) =>
        Some(CanonicalInput(t, NVGetPayload(p), getSource, CfEncoding, ip, ua, rfr, Nil, None)) // No headers or separate userId.
      }
    }

    // 3. Row not recognised
    case _ => "Line does not match CloudFront header or data row formats".failNel[Option[CanonicalInput]]
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
  private def toTimestamp(date: String, time: String): Validation[String, DateTime] =
    try {
      DateTime.parse("%sT%s+00:00".format(date, time)).success // Construct a UTC ISO date from CloudFront date and time
    } catch {
      case e => "Unexpected exception converting date [%s] and time [%s] to timestamp: [%s]".format(date, time, e.getMessage).fail
    }

  /**
   * Converts a raw querystring into a
   * GetPayload object.
   *
   * @param querystring The raw querystring
   * @return the GetPayload object, or
   *         an error, all wrapped in a
   *         Scalaz Validation
   */
  private def toGetPayload(querystring: String): ValidatedNameValueNel = toOption(querystring) match {
    case Some(qs) => TrackerPayload.extractGetPayload(qs, CfEncoding)
    case None => "Querystring is empty, cannot extract GET payload".fail
  }

  /**
   * Checks whether a String field is a hyphen
   * "-", which is used by CloudFront to signal
   * a null.
   *
   * @param field The field to check
   * @return True if the String was a hyphen "-"
   */
  private def toOption(field: String): Option[String] = Option(field) match {
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
  private def toCleanUri(uri: String): String = 
    StringUtils.removeEnd(uri, "%")
}
