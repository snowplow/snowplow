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

import cats.syntax.either._
import com.snowplowanalytics.iglu.core.{SchemaCriterion, SchemaKey}
import io.circe._

/** Trait inherited by every enrichment config case class */
trait Enrichment {

  /**
   * Gets the list of files the enrichment requires cached locally. The default implementation
   * returns an empty list; if an enrichment requires files, it must override this method.
   * @return A list of pairs, where the first entry in the pair indicates the (remote) location of
   * the source file and the second indicates the local path where the enrichment expects to find
   * the file.
   */
  def filesToCache: List[(URI, String)] = List.empty
}

/** Trait to hold helpers relating to enrichment config */
trait ParseableEnrichment {
  val supportedSchema: SchemaCriterion

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
}
