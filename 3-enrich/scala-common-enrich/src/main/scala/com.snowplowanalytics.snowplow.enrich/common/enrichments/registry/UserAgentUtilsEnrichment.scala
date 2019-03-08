/*Copyright (c) 2012-2019 Snowplow Analytics Ltd. All rights reserved.
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
package enrichments.registry

import scala.util.control.NonFatal

import com.snowplowanalytics.iglu.client.{SchemaCriterion, SchemaKey}
import eu.bitwalker.useragentutils._
import org.json4s.JValue
import org.slf4j.LoggerFactory
import scalaz._
import Scalaz._

object UserAgentUtilsEnrichmentConfig extends ParseableEnrichment {

  val supportedSchema = SchemaCriterion("com.snowplowanalytics.snowplow", "user_agent_utils_config", "jsonschema", 1, 0)
  private val log = LoggerFactory.getLogger(getClass())

  // Creates a UserAgentUtilsEnrichment instance from a JValue
  def parse(config: JValue, schemaKey: SchemaKey): ValidatedNelMessage[UserAgentUtilsEnrichment.type] = {
    log.warn(
      s"user_agent_utils enrichment is deprecated. Please visit here for more information: " +
        s"https://github.com/snowplow/snowplow/wiki/user-agent-utils-enrichment")
    isParseable(config, schemaKey).map(_ => UserAgentUtilsEnrichment)
  }
}

/**
 * Case class to wrap everything we
 * can extract from the useragent
 * using UserAgentUtils.
 *
 * Not to be declared inside a class Object
 * http://stackoverflow.com/questions/17270003/why-are-classes-inside-scala-package-objects-dispreferred
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

// Object and a case object with the same name

case object UserAgentUtilsEnrichment extends Enrichment {

  private val mobileDeviceTypes = Set(DeviceType.MOBILE, DeviceType.TABLET, DeviceType.WEARABLE)

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
      val b = ua.getBrowser
      val v = Option(ua.getBrowserVersion)
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
        deviceIsMobile = mobileDeviceTypes.contains(os.getDeviceType)
      ).success
    } catch {
      case NonFatal(e) => "Exception parsing useragent [%s]: [%s]".format(useragent, e.getMessage).fail
    }
}
