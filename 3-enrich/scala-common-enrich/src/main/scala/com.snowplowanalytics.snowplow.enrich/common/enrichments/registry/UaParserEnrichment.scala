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

import cats.{Eval, Id, Monad}
import cats.data.{EitherT, NonEmptyList, ValidatedNel}
import cats.effect.Sync
import cats.implicits._
import com.snowplowanalytics.iglu.core.{SchemaCriterion, SchemaKey}
import com.snowplowanalytics.snowplow.badrows._
import com.snowplowanalytics.snowplow.badrows.EnrichmentFailureMessage._
import io.circe._
import io.circe.syntax._
import ua_parser.Parser
import ua_parser.Client

import utils.CirceUtils

/** Companion object. Lets us create a UaParserEnrichment from a Json. */
object UaParserEnrichment extends ParseableEnrichment {
  override val supportedSchema =
    SchemaCriterion("com.snowplowanalytics.snowplow", "ua_parser_config", "jsonschema", 1, 0)
  private val localFile = "./ua-parser-rules.yml"

  override def parse(
    c: Json,
    schemaKey: SchemaKey,
    localMode: Boolean = false
  ): ValidatedNel[String, UaParserConf] =
    (for {
      _ <- isParseable(c, schemaKey).leftMap(NonEmptyList.one)
      rules <- getCustomRules(c).toEither
    } yield UaParserConf(schemaKey, rules)).toValidated

  /**
   * Creates a UaParserEnrichment from a UaParserConf
   * @param conf Configuration for the ua parser enrichment
   * @return a ua parser enrichment
   */
  def apply[F[_]: Monad: CreateUaParser](
    conf: UaParserConf
  ): EitherT[F, String, UaParserEnrichment] =
    EitherT(CreateUaParser[F].create(conf.uaDatabase.map(_._2)))
      .map(p => UaParserEnrichment(conf.schemaKey, p))

  private def getCustomRules(conf: Json): ValidatedNel[String, Option[(URI, String)]] =
    if (conf.hcursor.downField("parameters").downField("uri").focus.isDefined) {
      (for {
        uriAndDb <- (
          CirceUtils.extract[String](conf, "parameters", "uri").toValidatedNel,
          CirceUtils.extract[String](conf, "parameters", "database").toValidatedNel
        ).mapN((_, _)).toEither
        source <- getDatabaseUri(uriAndDb._1, uriAndDb._2).leftMap(NonEmptyList.one)
      } yield (source, localFile)).toValidated.map(_.some)
    } else {
      None.validNel
    }
}

/** Config for an ua_parser_config enrichment. Uses uap-java library to parse client attributes */
final case class UaParserEnrichment(schemaKey: SchemaKey, parser: Parser) extends Enrichment {
  private val enrichmentInfo = EnrichmentInformation(schemaKey, "ua-parser").some

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
  def extractUserAgent(useragent: String): Either[EnrichmentStageIssue, Json] =
    Either
      .catchNonFatal(parser.parse(useragent))
      .leftMap { e =>
        val msg = s"could not parse useragent: ${e.getMessage}"
        val f = InputDataEnrichmentFailureMessage("useragent", useragent.some, msg)
        EnrichmentFailure(enrichmentInfo, f)
      }
      .map(assembleContext)

  /** Assembles ua_parser_context from a parsed user agent. */
  def assembleContext(c: Client): Json = {
    // To display useragent version
    val useragentVersion = checkNull(c.userAgent.family) + prependSpace(c.userAgent.major) + prependDot(
      c.userAgent.minor
    ) + prependDot(c.userAgent.patch)

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

trait CreateUaParser[F[_]] {
  def create(uaFile: Option[String]): F[Either[String, Parser]]
}

object CreateUaParser {
  def apply[F[_]](implicit ev: CreateUaParser[F]): CreateUaParser[F] = ev

  implicit def syncCreateUaParser[F[_]: Sync]: CreateUaParser[F] = new CreateUaParser[F] {
    def create(uaFile: Option[String]): F[Either[String, Parser]] =
      Sync[F].delay { parser(uaFile) }
  }

  implicit def evalCreateUaParser: CreateUaParser[Eval] = new CreateUaParser[Eval] {
    def create(uaFile: Option[String]): Eval[Either[String, Parser]] =
      Eval.later { parser(uaFile) }
  }

  implicit def idCreateUaParser: CreateUaParser[Id] = new CreateUaParser[Id] {
    def create(uaFile: Option[String]): Id[Either[String, Parser]] =
      parser(uaFile)
  }

  private def constructParser(input: Option[InputStream]) = input match {
    case Some(is) =>
      try {
        new Parser(is)
      } finally {
        is.close()
      }
    case None => new Parser()
  }

  private def parser(file: Option[String]): Either[String, Parser] =
    (for {
      input <- Either.catchNonFatal(file.map(new FileInputStream(_)))
      p <- Either.catchNonFatal(constructParser(input))
    } yield p).leftMap(e => s"Failed to initialize ua parser: [${e.getMessage}]")
}
