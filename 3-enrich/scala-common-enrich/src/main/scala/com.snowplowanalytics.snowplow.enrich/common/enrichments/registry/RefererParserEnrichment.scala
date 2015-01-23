/*
 * Copyright (c) 2014 Snowplow Analytics Ltd. All rights reserved.
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

// Java
import java.net.URI

// Maven Artifact
import org.apache.maven.artifact.versioning.DefaultArtifactVersion

// Scalaz
import scalaz._
import Scalaz._

// json4s
import org.json4s.JValue

// Iglu
import iglu.client.{
  SchemaCriterion,
  SchemaKey
}
import iglu.client.validation.ProcessingMessageMethods._

// Snowplow referer-parser
import com.snowplowanalytics.refererparser.scala.{Parser => RefererParser}
import com.snowplowanalytics.refererparser.scala.Referer

// This project
import utils.{ConversionUtils => CU}
import utils.MapTransformer
import utils.MapTransformer._
import utils.ScalazJson4sUtils

/**
 * Companion object. Lets us create a
 * RefererParserEnrichment from a JValue
 */
object RefererParserEnrichment extends ParseableEnrichment {

  val supportedSchema = SchemaCriterion("com.snowplowanalytics.snowplow", "referer_parser", "jsonschema", 1, 0)

  /**
   * Creates a RefererParserEnrichment instance from a JValue.
   * 
   * @param config The referer_parser enrichment JSON
   * @param schemaKey The SchemaKey provided for the enrichment
   *        Must be a supported SchemaKey for this enrichment   
   * @return a configured RefererParserEnrichment instance
   */
  def parse(config: JValue, schemaKey: SchemaKey): ValidatedNelMessage[RefererParserEnrichment] = {
    isParseable(config, schemaKey).flatMap( conf => {
      (for {
        param  <- ScalazJson4sUtils.extract[List[String]](config, "parameters", "internalDomains")
        enrich =  RefererParserEnrichment(param)
      } yield enrich).toValidationNel
    })
  }

}

/**
 * Config for a referer_parser enrichment
 *
 * @param domains List of internal domains
 */
case class RefererParserEnrichment(
  domains: List[String]
  ) extends Enrichment {

  val version = new DefaultArtifactVersion("0.1.0")

  /**
   * A Scalaz Lens to update the term within
   * a Referer object.
   */
  private val termLens: Lens[Referer, MaybeString] = Lens.lensu((r, newTerm) => r.copy(term = newTerm), _.term)

  /**
   * Extract details about the referer (sic).
   *
   * Uses the referer-parser library. 
   *
   * @param uri The referer URI to extract
   *            referer details from
   * @param pageHost The host of the current
   *                 page (used to determine
   *                 if this is an internal
   *                 referer)
   * @return a Tuple3 containing referer medium,
   *         source and term, all Strings
   */
  def extractRefererDetails(uri: URI, pageHost: String): Option[Referer] = {
    for {
      r <- RefererParser.parse(uri, pageHost, domains)
      t = r.term.flatMap(t => CU.fixTabsNewlines(t))
    } yield termLens.set(r, t)
  }
}
