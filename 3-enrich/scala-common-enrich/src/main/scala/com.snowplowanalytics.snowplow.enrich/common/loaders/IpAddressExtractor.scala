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
package com.snowplowanalytics.snowplow.enrich.common.loaders

// Scala
import scala.annotation.tailrec

/**
 * Gets the true IP address events forwarded to the Scala Stream Collector.
 * See https://github.com/snowplow/snowplow/issues/1372
 */
object IpAddressExtractor {

  private val IpExtractionRegex = """^x-forwarded-for: ((?:[0-9]|\.)+).*""".r

  /**
   * If a request has been forwarded, extract the original client IP address;
   * otherwise return the standard IP address
   *
   * @param headers List of headers potentially containing X-FORWARDED-FOR
   * @param lastIp Fallback IP address if no XI-FORWARDED-FOR header exists
   * @return True client IP address
   */
  @tailrec
  def extractIpAddress(headers: List[String], lastIp: String): String = {
    headers match {
      case h :: t => h.toLowerCase match {
        case IpExtractionRegex(originalIpAddress) => originalIpAddress
        case _ => extractIpAddress(t, lastIp)
      }
      case Nil => lastIp
    }
  }
}
