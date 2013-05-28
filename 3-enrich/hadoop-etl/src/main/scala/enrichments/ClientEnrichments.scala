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
package enrichments

// Java
import java.lang.{Integer => JInteger}

// Scalaz
import scalaz._
import Scalaz._

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
   * for a screen resolution - for
   * details see:
   *
   * https://github.com/snowplow/snowplow/wiki/snowplow-tracker-protocol#wiki-browserandos
   */
  private val ResRegex = """(\d+)x(\d+)""".r

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
      browserName: String,
      browserFamily: String,
      browserVersion: Option[String],
      browserType: String,
      browserRenderEngine: String,
      // OS the browser is running on
      osName: String,
      osFamily: String,
      osManufacturer: String,
      // Hardware the OS is running on
      deviceType: String,
      deviceIsMobile: Boolean)

  /**
   * Extracts view dimensions (e.g. screen resolution,
   * browser/app viewport) stored as per the Tracker
   * Protocol:
   *
   * https://github.com/snowplow/snowplow/wiki/snowplow-tracker-protocol#wiki-browserandos
   *
   * @param field The name of the field
   *        holding the screen dimensions
   * @param res The packed string
   *        holding the screen dimensions
   * @return the ResolutionTuple or an
   *         error message, boxed in a
   *         Scalaz Validation
   */
  val extractViewDimensions: (String, String) => Validation[String, ViewDimensionsTuple] = (field, res) =>
    res match {
      case ResRegex(width, height) =>
        try {
          (width.toInt: JInteger, height.toInt: JInteger).success
        } catch {
          case _ => "Field [%s]: view dimensions [%s] exceed Integer's max range".format(field, res).fail
        }
      case _ => "Field [%s]: [%s] does not contain valid view dimensions".format(field, res).fail
    }

  /**
   * Extracts the client attributes
   * from a useragent string, using
   * UserAgentUtils.
   *
   * TODO: rewrite this when we swap
   * out UserAgentUtils for ua-parser
   *
   * @param useragent The useragent
   *        String to extract from.
   *        Should be encoded (i.e.
   *        not previously decoded).
   * @return the ClientAttributes or
   *         the message of the
   *         exception, boxed in a
   *         Scalaz Validation
   */
  def extractClientAttributes(useragent: String): Validation[String, ClientAttributes] = 

    try {
      val ua = UserAgent.parseUserAgentString(useragent)
      val b  = ua.getBrowser
      val v  = Option(ua.getBrowserVersion)
      val os = ua.getOperatingSystem

      ClientAttributes(
        browserName = b.getName,
        browserFamily = b.getGroup.getName,
        browserVersion = v map { _.getVersion },
        browserType = b.getBrowserType.getName,
        browserRenderEngine = b.getRenderingEngine.toString,
        osName = os.getName,
        osFamily = os.getGroup.getName,
        osManufacturer = os.getManufacturer.getName,
        deviceType = os.getDeviceType.getName,
        deviceIsMobile = os.isMobileDevice).success
    } catch {
      case e => "Exception parsing useragent [%s]: [%s]".format(useragent, e.getMessage).fail
    }
}