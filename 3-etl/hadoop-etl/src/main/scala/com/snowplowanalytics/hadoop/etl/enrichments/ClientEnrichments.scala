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
 * client - where the client is the
 * software which is using the SnowPlow
 * tracker.
 *
 * Enrichments relate to the useragent,
 * browser resolution, etc.
 */
object ClientEnrichments {
  
  /**
   * The Tracker Protocol's pattern
   * for a screen resolution.
   *
   * See for details:
   *
   * https://github.com/snowplow/snowplow/wiki/snowplow-tracker-protocol#wiki-browserandos
   */
  private val ResRegex = """(\d+)x(\d+)""".r

  /**
   * Case class to capture a client's
   * screen resolution
   */
  case class ScreenResolution(
    val width: Int,
    val height: Int)

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

  // TODO: write the extractor.

  /**
   * Extracts the screen resolution
   * from the packed format used by
   * the Tracker Protocol:
   *
   * https://github.com/snowplow/snowplow/wiki/snowplow-tracker-protocol#wiki-browserandos
   *
   * TODO: update this to return a
   * Scalaz Validation instead of Option
   *
   * @param res The resolution string
   * @return the ScreenResolution,
   *         Option-boxed, or None if
   *         we had a problem
   */
  def extractScreenResolution(res: String): Option[ScreenResolution] = res match {
    case ResRegex(h, w) => Some(ScreenResolution(h.toInt, w.toInt))
    case _ => None // TODO: change to "Could not extract screen resolution [%s]" format res
  }

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