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

/**
 * Module to hold specific helpers related to the
 * CloudFront input format.
 */
object CloudFrontLoader extends CollectorLoader {

  // Define the regular expression for extracting the fields
  // Adapted from Amazon's own cloudfront-loganalyzer.tgz
  private val w = "[\\s]+"    // Whitespace regex
  private val ow = "(?:" + w  // Optional whitespace begins
  private val CfRegex =     "([\\S]+)" + // Date          / date
                        w + "([\\S]+)" + // Time          / time
                        w + "([\\S]+)" + // EdgeLocation  / x-edge-location
                        w + "([\\S]+)" + // BytesSent     / sc-bytes
                        w + "([\\S]+)" + // IPAddress     / c-ip
                        w + "([\\S]+)" + // Operation     / cs-method
                        w + "([\\S]+)" + // Domain        / cs(Host)
                        w + "([\\S]+)" + // Object        / cs-uri-stem
                        w + "([\\S]+)" + // HttpStatus    / sc-status
                        w + "([\\S]+)" + // Referrer      / cs(Referer)
                        w + "([\\S]+)" + // UserAgent     / cs(User Agent)
                        w + "([\\S]+)" + // Querystring   / cs-uri-query
                        ow + "[\\S]+"  + // CookieHeader  / cs(Cookie)         added 12 Sep 2012
                        w +  "[\\S]+"  + // ResultType    / x-edge-result-type added 12 Sep 2012
                        w +  "[\\S]+)?".r // X-Amz-Cf-Id   / x-edge-request-id  added 12 Sep 2012

  /**
   * Converts the source string
   * into a CanonicalInput.
   *
   * @param line The line of data to convert
   * @return a CanonicalInput object
   */
  def toCanonicalInput(line: String): CanonicalInput = {

    // Stub a canonical input
    CanonicalInput(
      timestamp = null, // Placeholder
      payload = GetPayload("blah"), // Placeholder
      ipAddress = "128.0.0.1", // Placeholder
      userAgent = "BOT", // Placeholder
      refererUrl = None, // Placeholder
      userId = None // Placeholder
      )
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
   * TODO: move this into a CloudFront-specific file
   *
   * @param s The String to clean
   * @return the cleaned string
   */
  private def cleanUri(uri: String): String = 
    StringUtils.removeEnd(uri, "%")
}