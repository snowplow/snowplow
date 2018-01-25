/*
 * Copyright (c) 2012-2018 Snowplow Analytics Ltd. All rights reserved.
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

// json4s
import org.json4s._
import org.json4s.JsonDSL._

// Iglu
import iglu.client.{
  SchemaCriterion,
  SchemaKey
}

// This project
import utils.ScalazJson4sUtils

object HttpHeaderExtractorEnrichmentConfig extends ParseableEnrichment {
  val supportedSchema = SchemaCriterion("com.snowplowanalytics.snowplow.enrichments", "http_header_extractor_config", "jsonschema", 1, 0)

  /**
   * Creates a HttpHeaderExtractorEnrichment instance from a JValue.
   *
   * @param config The header_extractor enrichment JSON
   * @param schemaKey The SchemaKey provided for the enrichment
   *        Must be a supported SchemaKey for this enrichment
   * @return a configured HeaderExtractorEnrichment instance
   */
  def parse(config: JValue, schemaKey: SchemaKey): ValidatedNelMessage[HttpHeaderExtractorEnrichment] = {
    isParseable(config, schemaKey).flatMap(conf => {
      (for {
        headersPattern <- ScalazJson4sUtils.extract[String](config, "parameters", "headersPattern")
        enrich = HttpHeaderExtractorEnrichment(headersPattern)
      } yield enrich).toValidationNel
    })
  }
}

/**
 * Enrichment extracting certain headers from headers.
 *
 * @param headersPattern Names of the headers to be extracted
 */
case class HttpHeaderExtractorEnrichment(
  headersPattern: String) extends Enrichment {

  case class Header(name: String, value: String)

  val version = new DefaultArtifactVersion("0.1.0")

  def extract(headers: List[String]): List[JsonAST.JObject] = {
    val httpHeaders = headers.flatMap { header =>
      header.split(":", 2) match {
        case Array(name, value) if name.matches(headersPattern) =>
          Some(Header(name, value))
        case _ => None
      }
    }

    httpHeaders.map { header =>
      (("schema" -> "iglu:org.ietf/http_header/jsonschema/1-0-0") ~
        ("data" ->
          ("name" -> header.name.trim) ~
          ("value" -> header.value.trim)))
    }
  }
}
