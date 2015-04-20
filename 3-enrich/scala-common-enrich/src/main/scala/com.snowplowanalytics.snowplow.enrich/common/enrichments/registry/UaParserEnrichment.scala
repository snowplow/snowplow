/*Copyright (c) 2012-2015 Snowplow Analytics Ltd. All rights reserved.
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
package enrichments
package registry

// Maven Artifact
import org.apache.maven.artifact.versioning.DefaultArtifactVersion

// Scalaz
import scalaz._
import Scalaz._

// ua-parser
import ua_parser.Parser
import ua_parser.Client
import ua_parser.Client

// json4s
import org.json4s._
import org.json4s.JValue
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

// Iglu
import iglu.client.{
  SchemaCriterion,
  SchemaKey
}
import utils.ScalazJson4sUtils

/**
* Companion object. Lets us create a UaParserEnrichment
* from a JValue.
*/
object UaParserEnrichmentConfig extends ParseableEnrichment {

  val supportedSchema = SchemaCriterion("com.snowplowanalytics.snowplow", "ua_parser_config", "jsonschema", 1, 0)

  def parse(config: JValue, schemaKey: SchemaKey): ValidatedNelMessage[UaParserEnrichment.type] =
    isParseable(config, schemaKey).map(_ => UaParserEnrichment)
}

/**
* Config for an ua_parser_config enrichment
*
* Uses uap-java library to parse client attributes
*/
case object UaParserEnrichment extends Enrichment {

  val version = new DefaultArtifactVersion("0.1.0")

  val uaParser = new Parser()

  /*
  * Adds a period in front of a not-null version element
  */
  def prependDot(versionElement: String): String = {
    if (versionElement != null) {
      "." + versionElement
    } else {
      ""
    } 
  }
  
  /*
  * Prepends space before the versionElement
  */
  def prependSpace(versionElement: String): String = {
    if (versionElement != null) {
      " " + versionElement
    } else {
      ""
    }
  }

  /*
  * Checks for null value in versionElement for family parameter
  */
  def checkNull(versionElement: String): String = {
    if (versionElement == null) {
      ""
    } else {
      versionElement
    }
  }
  /**
  * Extracts the client attributes
  * from a useragent string, using
  * UserAgentEnrichment.
  *
  * @param useragent The useragent
  *        String to extract from.
  *        Should be encoded (i.e.
  *        not previously decoded).
  * @return the json or
  *         the message of the
  *         exception, boxed in a
  *         Scalaz Validation
  */
  def extractUserAgent(useragent: String): Validation[String, JsonAST.JObject] = {

            val c = try {
              uaParser.parse(useragent)
            } catch {
                 case e => return "Exception parsing useragent [%s]: [%s]".format(useragent, e.getMessage).fail
            }
            // To display useragent version
            val useragentVersion = checkNull(c.userAgent.family) + prependSpace(c.userAgent.major) + prependDot(c.userAgent.minor) + prependDot(c.userAgent.patch)

            // To display operating system version
            val osVersion = checkNull(c.os.family) + prependSpace(c.os.major) + prependDot(c.os.minor) + prependDot(c.os.patch) + prependDot(c.os.patchMinor)

            val json =
                      ( ("schema" -> "iglu:com.snowplowanalytics.snowplow/ua_parser_context/jsonschema/1-0-0") ~
                        ("data" ->
                          ("useragentFamily" -> c.userAgent.family) ~
                          ("useragentMajor" -> c.userAgent.major) ~
                          ("useragentMinor" -> c.userAgent.minor) ~
                          ("useragentPatch" -> c.userAgent.patch) ~
                          ("useragentVersion" -> useragentVersion) ~
                          ("osFamily" -> c.os.family) ~
                          ("osMajor" -> c.os.major) ~
                          ("osMinor" -> c.os.minor) ~
                          ("osPatch" -> c.os.patch) ~
                          ("osPatchMinor" -> c.os.patchMinor) ~
                          ("osVersion" -> osVersion) ~
                          ("deviceFamily" -> c.device.family)
                        )
                      )

            json.success
  }
}
