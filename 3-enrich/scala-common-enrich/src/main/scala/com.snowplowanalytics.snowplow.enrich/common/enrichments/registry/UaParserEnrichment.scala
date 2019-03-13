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

import java.io.{FileInputStream, InputStream}
import java.net.URI

import scala.util.control.NonFatal

import com.snowplowanalytics.iglu.client.{SchemaCriterion, SchemaKey}
import com.snowplowanalytics.iglu.client.validation.ProcessingMessageMethods._
import io.circe._
import io.circe.syntax._
import scalaz._
import Scalaz._
import ua_parser.Parser
import ua_parser.Client

import utils.{ConversionUtils, ScalazCirceUtils}

/** Companion object. Lets us create a UaParserEnrichment from a Json. */
object UaParserEnrichmentConfig extends ParseableEnrichment {
  val supportedSchema =
    SchemaCriterion("com.snowplowanalytics.snowplow", "ua_parser_config", "jsonschema", 1, 0)

  private val localRulefile = "./ua-parser-rules.yml"

  def parse(config: Json, schemaKey: SchemaKey): ValidatedNelMessage[UaParserEnrichment] =
    isParseable(config, schemaKey).flatMap { conf =>
      (for {
        rules <- getCustomRules(conf)
      } yield UaParserEnrichment(rules)).toValidationNel
    }

  private def getCustomRules(conf: Json): ValidatedMessage[Option[(URI, String)]] =
    if (conf.hcursor.downField("parameters").downField("uri").focus.isDefined) {
      for {
        uri <- ScalazCirceUtils.extract[String](conf, "parameters", "uri")
        db <- ScalazCirceUtils.extract[String](conf, "parameters", "database")
        source <- getUri(uri, db)
      } yield (source, localRulefile).some
    } else {
      None.success
    }

  private def getUri(uri: String, database: String): ValidatedMessage[URI] =
    ConversionUtils
      .stringToUri(uri + (if (uri.endsWith("/")) "" else "/") + database)
      .flatMap {
        case Some(u) => u.success
        case None => "A valid URI to ua-parser regex file must be provided".fail
      }
      .toProcessingMessage
}

/** Config for an ua_parser_config enrichment. Uses uap-java library to parse client attributes */
final case class UaParserEnrichment(customRulefile: Option[(URI, String)]) extends Enrichment {
  override val filesToCache: List[(URI, String)] =
    customRulefile.map(List(_)).getOrElse(List.empty)

  lazy val uaParser = {
    def constructParser(input: Option[InputStream]) = input match {
      case Some(is) =>
        try {
          new Parser(is)
        } finally {
          is.close()
        }
      case None => new Parser()
    }

    def tryWithCatch[T](a: => T): Validation[Throwable, T] =
      try {
        a.success
      } catch {
        case NonFatal(e) => e.failure
      }

    val parser = for {
      input <- tryWithCatch(customRulefile.map(f => new FileInputStream(f._2)))
      p <- tryWithCatch(constructParser(input))
    } yield p
    parser.leftMap(e => s"Failed to initialize ua parser: [${e.getMessage}]")
  }

  /** Adds a period in front of a not-null version element */
  def prependDot(versionElement: String): String =
    if (versionElement != null) {
      "." + versionElement
    } else {
      ""
    }

  /** Prepends space before the versionElement */
  def prependSpace(versionElement: String): String =
    if (versionElement != null) {
      " " + versionElement
    } else {
      ""
    }

  /** Checks for null value in versionElement for family parameter */
  def checkNull(versionElement: String): String =
    if (versionElement == null) {
      ""
    } else {
      versionElement
    }

  /**
   * Extracts the client attributes from a useragent string, using UserAgentEnrichment.
   * @param useragent to extract from. Should be encoded, i.e. not previously decoded.
   * @return the json or the message of the exception, boxed in a Scalaz Validation
   */
  def extractUserAgent(useragent: String): Validation[String, Json] =
    for {
      parser <- uaParser
      c <- try {
        parser.parse(useragent).success
      } catch {
        case NonFatal(e) => s"Exception parsing useragent [$useragent]: [${e.getMessage}]".fail
      }
    } yield assembleContext(c)

  /** Assembles ua_parser_context from a parsed user agent. */
  def assembleContext(c: Client): Json = {
    // To display useragent version
    val useragentVersion = checkNull(c.userAgent.family) + prependSpace(c.userAgent.major) + prependDot(
      c.userAgent.minor) + prependDot(c.userAgent.patch)

    // To display operating system version
    val osVersion = checkNull(c.os.family) + prependSpace(c.os.major) + prependDot(c.os.minor) +
      prependDot(c.os.patch) + prependDot(c.os.patchMinor)

    def getJson(s: String): Json =
      Option(s).map(Json.fromString).getOrElse(Json.Null)

    Json.obj(
      "schema" :=
        Json.fromString("iglu:com.snowplowanalytics.snowplow/ua_parser_context/jsonschema/1-0-0"),
      "data" := Json.obj(
        "useragentFamily" := getJson(c.userAgent.family),
        "useragentMajor" := getJson(c.userAgent.major),
        "useragentMinor" := getJson(c.userAgent.minor),
        "useragentPatch" := getJson(c.userAgent.patch),
        "useragentVersion" := getJson(useragentVersion),
        "osFamily" := getJson(c.os.family),
        "osMajor" := getJson(c.os.major),
        "osMinor" := getJson(c.os.minor),
        "osPatch" := getJson(c.os.patch),
        "osPatchMinor" := getJson(c.os.patchMinor),
        "osVersion" := getJson(osVersion),
        "deviceFamily" := getJson(c.device.family)
      )
    )
  }
}
