/*
 * Copyright (c) 2014-2020 Snowplow Analytics Ltd. All rights reserved.
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

import java.net.URI

import cats.Monad
import cats.data.{EitherT, NonEmptyList, ValidatedNel}
import cats.implicits._
import com.snowplowanalytics.iglu.core.{SchemaCriterion, SchemaKey}
import com.snowplowanalytics.refererparser._
import io.circe.Json

import utils.{ConversionUtils => CU}
import utils.CirceUtils

/** Companion object. Lets us create a RefererParserEnrichment from a Json */
object RefererParserEnrichment extends ParseableEnrichment {
  override val supportedSchema =
    SchemaCriterion("com.snowplowanalytics.snowplow", "referer_parser", "jsonschema", 2, 0)

  private val localFile = "./referer-parser.json"

  /**
   * Creates a RefererParserConf from a Json.
   * @param c The referer_parser enrichment JSON
   * @param schemaKey provided for the enrichment, must be supported by this enrichment
   * @return a referer parser enrichment configuration
   */
  override def parse(
    c: Json,
    schemaKey: SchemaKey,
    localMode: Boolean
  ): ValidatedNel[String, RefererParserConf] =
    (for {
      _ <- isParseable(c, schemaKey).leftMap(NonEmptyList.one)
      // better-monadic-for
      conf <- (
        CirceUtils.extract[String](c, "parameters", "uri").toValidatedNel,
        CirceUtils.extract[String](c, "parameters", "database").toValidatedNel,
        CirceUtils.extract[List[String]](c, "parameters", "internalDomains").toValidatedNel
      ).mapN { (uri, db, domains) =>
        (uri, db, domains)
      }.toEither
      source <- getDatabaseUri(conf._1, conf._2).leftMap(NonEmptyList.one)
    } yield RefererParserConf(file(source, conf._2, localFile, localMode), conf._3)).toValidated

  private def file(
    uri: URI,
    db: String,
    localFile: String,
    localMode: Boolean
  ): (URI, String) =
    if (localMode) (uri, getClass.getResource(db).toURI.getPath)
    else (uri, localFile)

  /**
   * Creates a RefererParserEnrichment from a RefererParserConf
   * @param conf Configuration for the referer parser enrichment
   * @return a referer parser enrichment
   */
  def apply[F[_]: Monad: CreateParser](
    conf: RefererParserConf
  ): EitherT[F, String, RefererParserEnrichment] =
    EitherT(CreateParser[F].create(conf.refererDatabase._2))
      .leftMap(_.getMessage)
      .map(p => RefererParserEnrichment(p, conf.internalDomains))
}

/**
 * Config for a referer_parser enrichment
 * @param parser Referer parser
 * @param domains List of internal domains
 */
final case class RefererParserEnrichment(parser: Parser, domains: List[String]) extends Enrichment {

  /**
   * Extract details about the referer (sic). Uses the referer-parser library.
   * @param uri The referer URI to extract referer details from
   * @param pageHost The host of the current page (used to determine if this is an internal referer)
   * @return a Tuple3 containing referer medium, source and term, all Strings
   */
  def extractRefererDetails(uri: URI, pageHost: String): Option[Referer] =
    parser.parse(uri, Option(pageHost), domains).map {
      case SearchReferer(m, s, t) =>
        val fixedTerm = t.flatMap(CU.fixTabsNewlines)
        SearchReferer(m, s, fixedTerm)
      case o => o
    }
}
