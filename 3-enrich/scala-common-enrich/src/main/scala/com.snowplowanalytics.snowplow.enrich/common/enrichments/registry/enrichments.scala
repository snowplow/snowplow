/*
 * Copyright (c) 2012-2014 Snowplow Analytics Ltd. All rights reserved.
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

// Scalaz
import scalaz._
import Scalaz._

// Maven Artifact
import org.apache.maven.artifact.versioning.DefaultArtifactVersion

// json4s
import org.json4s.JValue

// Iglu
import iglu.client.{
  SchemaCriterion,
  SchemaKey
}
import iglu.client.validation.ProcessingMessageMethods._

// This project
import utils.ScalazJson4sUtils

/**
 * Trait inherited by every enrichment config case class
 */
trait Enrichment {
  val version: DefaultArtifactVersion
}

/**
 * Trait to hold helpers relating to enrichment config
 */
trait ParseableEnrichment {

  val supportedSchema: SchemaCriterion

  /**
   * Tests whether a JSON is parseable by a
   * specific EnrichmentConfig constructor
   *
   * @param config The JSON
   * @param schemaKey The schemaKey which needs
   *        to be checked
   * @return The JSON or an error message, boxed
   */
  def isParseable(config: JValue, schemaKey: SchemaKey): ValidatedNelMessage[JValue] = {
    if (supportedSchema matches schemaKey) {
      config.success
    } else {
      ("Schema key %s is not supported. A '%s' enrichment must have schema '%s'.")
        .format(schemaKey, supportedSchema.name, supportedSchema)
        .toProcessingMessage.fail.toValidationNel
    }
  }
}
