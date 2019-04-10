/*
 * Copyright (c) 2012-2019 Snowplow Analytics Ltd. All rights reserved.
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
import io.circe._

import utils.ConversionUtils

/** Trait inherited by every enrichment config case class */
trait Enrichment

sealed trait EnrichmentConf {
  def filesToCache: List[(URI, String)]
}
final case class RefererParserConf(
  filesToCache: List[(URI, String)],
  internalDomains: List[String]
) extends EnrichmentConf

/** Trait to hold helpers relating to enrichment config */
trait ParseableEnrichment {

  /** The schemas supported by this enrichment */
  def supportedSchema: SchemaCriterion

  /**
   * Tentatively parses an enrichment configuration and sends back the files that need to be cached
   * prior to the EnrichmentRegistry construction.
   * @param config Json configuration for the enrichment
   * @param schemaKey Version of the schema we want to run
   * @return the configuration for this enrichment as well as the list of files it needs cached
   */
  def parse(
    config: Json,
    schemaKey: SchemaKey
  ): ValidatedNel[String, EnrichmentConf]

  /**
   * Gets the list of files the enrichment requires cached locally. The default implementation
   * returns an empty list; if an enrichment requires files, it must override this method.
   * @return A list of pairs, where the first entry in the pair indicates the (remote) location of
   * the source file and the second indicates the local path where the enrichment expects to find
   * the file.
   */
  def filesToCache: List[(URI, String)] = List.empty

  /**
   * Tests whether a JSON is parseable by a specific EnrichmentConfig constructor
   * @param config The JSON
   * @param schemaKey The schemaKey which needs to be checked
   * @return The JSON or an error message, boxed
   */
  def isParseable(config: Json, schemaKey: SchemaKey): Either[String, Json] =
    if (supportedSchema.matches(schemaKey)) {
      config.asRight
    } else {
      ("Schema key %s is not supported. A '%s' enrichment must have schema '%s'.")
        .format(schemaKey, supportedSchema.name, supportedSchema)
        .asLeft
    }

  /**
   * Convert the path to a file from a String to a URI.
   * @param uri URI to a database file
   * @param database Name of the database
   * @return an Either-boxed URI
   */
  protected def getDatabaseUri(uri: String, database: String): Either[String, URI] =
    ConversionUtils
      .stringToUri(uri + (if (uri.endsWith("/")) "" else "/") + database)
      .flatMap {
        case Some(u) => u.asRight
        case None => "URI to IAB file must be provided".asLeft
      }
}
