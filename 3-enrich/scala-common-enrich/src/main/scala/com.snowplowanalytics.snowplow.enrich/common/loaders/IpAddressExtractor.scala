/*
 * Copyright (c) 2012-2018 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.enrich.common.loaders

// Scala
import scala.annotation.tailrec

/**
 * Gets the true IP address events forwarded to the Scala Stream Collector.
 * See https://github.com/snowplow/snowplow/issues/1372
 */
object IpAddressExtractor {

  private val ipRegex            = """\"?\[?((?:[0-9a-f]|\.|\:+)+).*\]?\"?"""
  private val XForwardedForRegex = s"""^x-forwarded-for: $ipRegex.*""".r
  private val ForwardedForRegex  = s"""^forwarded: for=$ipRegex.*""".r
  private val CloudfrontRegex    = s"""^$ipRegex.*""".r

  /**
   * If a request has been forwarded, extract the original client IP address;
   * otherwise return the standard IP address
   *
   * @param headers List of headers potentially containing X-FORWARDED-FOR or FORWARDED
   * @param lastIp Fallback IP address if no X-FORWARDED-FOR or FORWARDED header exists
   * @return True client IP address
   */
  @tailrec
  def extractIpAddress(headers: List[String], lastIp: String): String =
    headers match {
      case h :: t =>
        h.toLowerCase match {
          case XForwardedForRegex(originalIpAddress) => originalIpAddress
          case ForwardedForRegex(originalIpAddress)  => originalIpAddress
          case _                                     => extractIpAddress(t, lastIp)
        }
      case Nil => lastIp
    }

  /**
   * If a request has been forwarded, extract the original client IP address;
   * otherwise return the standard IP address
   *
   * @param xForwardedFor x-forwarded-for field from the Cloudfront log
   * @param lastIp Fallback IP address if no X-FORWARDED-FOR header exists
   * @return True client IP address
   */
  def extractIpAddress(xForwardedFor: String, lastIp: String): String = xForwardedFor match {
    case CloudfrontRegex(originalIpAddress) => originalIpAddress
    case _                                  => lastIp
  }
}
