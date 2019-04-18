/**
 * Copyright (c) 2019-2019 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics
package snowplow
package enrich
package common
package enrichments.registry

// Json4s
import org.json4s.{DefaultFormats, Extraction, JObject, JValue}
import org.json4s.JsonDSL._
import com.snowplowanalytics.snowplow.enrich.common.utils.ScalazJson4sUtils

// Scalaz
import scalaz._
import Scalaz._

// Iglu
import iglu.client.SchemaKey
import iglu.client.SchemaCriterion

// Yauaa
import nl.basjes.parse.useragent.UserAgent
import nl.basjes.parse.useragent.UserAgentAnalyzer

// Scala
import scala.collection.JavaConverters._

/** Companion object to create an instance of YauaaEnrichment from the configuration. */
object YauaaEnrichment extends ParseableEnrichment {

  implicit val formats = DefaultFormats

  val supportedSchema =
    SchemaCriterion("com.snowplowanalytics.snowplow.enrichments", "yauaa_enrichment_config", "jsonschema", 1, 0)

  /**
   * Creates a YauaaEnrichment instance from a JValue containing the configuration of the enrichment.
   *
   * @param config    JSON containing configuration for YAUAA enrichment.
   * @param schemaKey SchemaKey provided for this enrichment.
   *                  Must be a supported SchemaKey for this enrichment.
   * @return Configured YauaaEnrichment instance
   */
  def parse(config: JValue, schemaKey: SchemaKey): ValidatedNelMessage[YauaaEnrichment] =
    isParseable(config, schemaKey).flatMap { _ =>
      val maybeCacheSize = ScalazJson4sUtils.extract[Int](config, "parameters", "cacheSize").toOption
      YauaaEnrichment(maybeCacheSize).success
    }

  /** Helper to decapitalize a string. Used for the names of the fields returned in the context. */
  def decapitalize(s: String): String = s match {
    case _ if s.isEmpty     => s
    case _ if s.length == 1 => s.toLowerCase
    case _                  => s.charAt(0).toLower + s.substring(1)
  }
}

/**
 * Class for YAUAA enrichment, which tries to parse and analyze the user agent string
 * and extract as many relevant attributes as possible, like for example the device class.
 *
 * @param cacheSize Amount of user agents already parsed that stay in cache for faster parsing.
 */
final case class YauaaEnrichment(cacheSize: Option[Int]) extends Enrichment {

  import YauaaEnrichment.decapitalize

  private val uaa: UserAgentAnalyzer = {
    val a = UserAgentAnalyzer
      .newBuilder()
      .build()
    cacheSize.foreach(a.setCacheSize)
    a
  }

  // For unit testing
  private[registry] def getCacheSize = uaa.getCacheSize

  private implicit val formats = DefaultFormats

  val contextSchema = "iglu:nl.basjes/yauaa_context/jsonschema/1-0-0"

  val defaultDeviceClass = "UNKNOWN"
  val defaultResult      = Map(decapitalize(UserAgent.DEVICE_CLASS) -> defaultDeviceClass)

  /**
   * Gets the result of YAUAA user agent analysis as self-describing JSON, for a specific event.
   * Any non-fatal error will return failure.
   *
   * @param userAgent User agent of the event.
   * @return Attributes retrieved thanks to the user agent (if any), as self-describing JSON.
   */
  def getYauaaContext(userAgent: String): Validation[String, JObject] = {
    val parsed = parseUserAgent(userAgent)
    Extraction.decompose(parsed) match {
      case obj: JObject => addSchema(obj).success
      case _            => s"Couldn't transform YAUAA fields [$parsed] into JSON".failure
    }
  }

  /** Gets the map of attributes retrieved by YAUAA from the user agent.
   * @return Map with all the fields extracted by YAUAA by parsing the user agent.
   *         If the input is null or empty, a map with just the DeviceClass set to UNKNOWN is returned.
   */
  def parseUserAgent(userAgent: String): Map[String, String] =
    userAgent match {
      case null | "" =>
        defaultResult
      case _ =>
        val parsedUA = uaa.parse(userAgent)
        parsedUA.getAvailableFieldNames.asScala
          .map(field => decapitalize(field) -> parsedUA.getValue(field))
          .toMap
    }

  /**
   * Add schema URI on Iglu to JSON Object
   *
   * @param context Yauaa context as JSON Object
   * @return Self-describing JSON with the result of YAUAA enrichment.
   */
  private def addSchema(context: JObject): JObject =
    ("schema", contextSchema) ~ (("data", context))
}
