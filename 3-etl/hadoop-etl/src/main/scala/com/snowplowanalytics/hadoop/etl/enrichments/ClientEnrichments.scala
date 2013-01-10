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
package enrichments

// UserAgentUtils
import nl.bitwalker.useragentutils._

/**
 * Contains enrichments related to the
 * client - i.e. useragent, browser
 * details etc.
 */
object ClientEnrichments {
  
  /**
   * Case class to wrap everything we
   * can extract from the useragent
   * using UserAgentUtils.
   *
   * TODO: update this definition when
   * we swap out UserAgentUtils for
   * ua-parser
   */
  case class ClientAttributes(
    // Browser
    val browserName: String,
    val browserFamily: String,
    val browserVersion: Option[String],
    val browserType: String,
    val browserRenderEngine: String,
    // OS the browser is running on
    val osName: String,
    val osFamily: String,
    val osManufacturer: String,
    // Hardware the OS is running on
    val deviceType: String,
    val deviceIsMobile: Boolean)

  /**
   * Extracts the client attributes
   * from a useragent string, using
   * UserAgentUtils.
   *
   * TODO: rewrite this when we swap
   * out UserAgentUtils for ua-parser
   *
   * TODO: update this to return a
   * Scalaz Validation instead of Option
   *
   * @param useragent The useragent
   *                  string
   * @return the ClientAttributes,
   *         Option-boxed, or None if
   *         we had a problem
   */
  def extractClientAttributes(useragent: String): Option[ClientAttributes] = try {
    val ua = UserAgent.parseUserAgentString(useragent)
    val b  = ua.getBrowser
    val v  = Option(ua.getBrowserVersion)
    val os = ua.getOperatingSystem

    Some(ClientAttributes(
      browserName = b.getName,
      browserFamily = b.getGroup.getName,
      browserVersion = v map { _.getVersion },
      browserType = b.getBrowserType.getName,
      browserRenderEngine = b.getRenderingEngine.toString,
      osName = os.getName,
      osFamily = os.getGroup.getName,
      osManufacturer = os.getManufacturer.getName,
      deviceType = os.getDeviceType.getName,
      deviceIsMobile = os.isMobileDevice))
    } catch {
      case _ => None // TODO: need to return the error
    }
}