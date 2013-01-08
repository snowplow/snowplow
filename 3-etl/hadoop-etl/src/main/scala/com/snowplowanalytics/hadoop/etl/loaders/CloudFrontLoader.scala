/*
 * Copyright (c) 2012 SnowPlow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.hadoop.etl
package loaders

// Apache Commons
import org.apache.commons.lang.StringUtils

// Joda-Time
import org.joda.time.{DateTime => JoDateTime}
import org.joda.time.format.{DateTimeFormat => JoDateTimeFormat,
                             DateTimeFormatter => JoDateTimeFormatter}

/**
 * Module to hold specific helpers related to the
 * CloudFront input format.
 */
object CloudFrontLoader extends CollectorLoader {

  // Define the regular expression for extracting the fields
  // Adapted from Amazon's own cloudfront-loganalyzer.tgz
  private val CfRegex = {
    val w = "[\\s]+"   // Whitespace regex
    val ow = "(?:" + w // Optional whitespace begins
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
   * CanonicalInput.
   *
   * TODO: need to change this to
   * handling some sort of validation
   * object.
   *
   * @param line A line of data to convert
   * @return a CanonicalInput object, Option-
   *         boxed, or None if no input was
   *         extractable.
   */
  def toCanonicalInput(line: String): Option[CanonicalInput] = line match {
    
    // 1. Header row
    case h if (h.startsWith("#Version:") ||
               h.startsWith("#Fields:"))    => None // TODO: would be nice to attach the reason
    
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
                 querystring,
                 _,
                 _,
                 _) =>

      /*
      // Is this a request for the tracker? Might be a browser favicon request or similar
      if (!isIceRequest(object)) return None // TODO: would be nice to attach the reason

      // TODO: pull this out so it's reusable by other implementations
      if (!(objct.startsWith("/ice.png") || objct.equals("/i") ||
           objct.startsWith("/i?"))) return None // TODO: would be nice to attach the reason
      */

      // isNullField(querystring)) { // Also works if Forward Query String = yes
      // TODO: need to check the other fields are set too

      // TODO: convert to YodaTime
      Some(CanonicalInput(timestamp = null, // Placeholder
                          payload   = GetPayload(querystring),
                          ipAddress = ipAddress,
                          userAgent = userAgent,
                          refererUrl = Some(referer),
                          userId = None))

    // 3. Row does not match CloudFront header or data row formats
    case _ => None // TODO: return a validation error so we can route this row to the bad row bin
  }

  /**
   * Checks whether a String field is a hyphen
   * "-", which is used by CloudFront to signal
   * a null.
   *
   * @param field The field to check
   * @return True if the String was a hyphen "-"
   */
  // def isNullField(str: String) {
  //  return (s == null || s.equals("") || s.equals("-"));
  // }

  /**
   * 'Cleans' a string to make it parsable by
   * URLDecoder.decode.
   * 
   * The '%' character seems to be appended to the
   * end of some URLs in the CloudFront logs, causing
   * Exceptions when using URLDecoder.decode. Perhaps
   * a CloudFront bug?
   *
   * TODO: move this into a CloudFront-specific file
   *
   * @param s The String to clean
   * @return the cleaned string
   */
  private def cleanUri(uri: String): String = 
    StringUtils.removeEnd(uri, "%")
}