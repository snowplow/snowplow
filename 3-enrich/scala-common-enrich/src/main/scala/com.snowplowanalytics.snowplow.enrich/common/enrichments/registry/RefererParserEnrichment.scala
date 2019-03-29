/*
 * Copyright (c) 2014-2019 Snowplow Analytics Ltd. All rights reserved.
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

import cats.data.ValidatedNel
import cats.syntax.either._
import com.snowplowanalytics.iglu.core.{SchemaCriterion, SchemaKey}
import com.snowplowanalytics.refererparser.scala.{Parser => RefererParser}
import com.snowplowanalytics.refererparser.scala.Referer
import io.circe._

import utils.{ConversionUtils => CU}
import utils.CirceUtils

/** Companion object. Lets us create a RefererParserEnrichment from a Json */
object RefererParserEnrichment extends ParseableEnrichment {
  val supportedSchema =
    SchemaCriterion("com.snowplowanalytics.snowplow", "referer_parser", "jsonschema", 1, 0)

  /**
   * Creates a RefererParserEnrichment instance from a Json.
   * @param c The referer_parser enrichment JSON
   * @param schemaKey provided for the enrichment, must be supported by this enrichment
   * @return a configured RefererParserEnrichment instance
   */
  def parse(
    c: Json,
    schemaKey: SchemaKey
  ): ValidatedNel[String, RefererParserEnrichment] =
    (for {
      _ <- isParseable(c, schemaKey)
      param <- CirceUtils.extract[List[String]](c, "parameters", "internalDomains").toEither
    } yield RefererParserEnrichment(param))
      .toValidatedNel
}

/**
 * Config for a referer_parser enrichment
 * @param domains List of internal domains
 */
final case class RefererParserEnrichment(domains: List[String]) extends Enrichment {

  /**
   * Extract details about the referer (sic). Uses the referer-parser library.
   * @param uri The referer URI to extract referer details from
   * @param pageHost The host of the current page (used to determine if this is an internal referer)
   * @return a Tuple3 containing referer medium, source and term, all Strings
   */
  def extractRefererDetails(uri: URI, pageHost: String): Option[Referer] =
    RefererParser.parse(uri, pageHost, domains).map { r =>
      val fixedTerm = r.term.flatMap(CU.fixTabsNewlines)
      r.copy(term = fixedTerm)
    }
}
