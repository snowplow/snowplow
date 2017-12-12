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

// Maven Artifact
import org.apache.maven.artifact.versioning.DefaultArtifactVersion

// Scalaz
import scalaz._
import Scalaz._

// json4s
import org.json4s._
import org.json4s.JsonDSL._

// Iglu
import iglu.client.{SchemaCriterion, SchemaKey}
// HttpClient
import org.apache.http.message.BasicHeaderValueParser

// This project
import utils.ScalazJson4sUtils

object CookieExtractorEnrichmentConfig extends ParseableEnrichment {
  val supportedSchema = SchemaCriterion("com.snowplowanalytics.snowplow", "cookie_extractor_config", "jsonschema", 1, 0)

  /**
   * Creates a CookieExtractorEnrichment instance from a JValue.
   *
   * @param config The cookie_extractor enrichment JSON
   * @param schemaKey The SchemaKey provided for the enrichment
   *        Must be a supported SchemaKey for this enrichment
   * @return a configured CookieExtractorEnrichment instance
   */
  def parse(config: JValue, schemaKey: SchemaKey): ValidatedNelMessage[CookieExtractorEnrichment] =
    isParseable(config, schemaKey).flatMap(conf => {
      (for {
        cookieNames <- ScalazJson4sUtils.extract[List[String]](config, "parameters", "cookies")
        enrich = CookieExtractorEnrichment(cookieNames)
      } yield enrich).toValidationNel
    })
}

/**
 * Enrichment extracting certain cookies from headers.
 *
 * @param cookieNames Names of the cookies to be extracted
 */
case class CookieExtractorEnrichment(
  cookieNames: List[String]
) extends Enrichment {

  def extract(headers: List[String]): List[JsonAST.JObject] = {
    // rfc6265 - sections 4.2.1 and 4.2.2

    val cookies = headers.flatMap { header =>
      header.split(":", 2) match {
        case Array("Cookie", value) =>
          val nameValuePairs = BasicHeaderValueParser.parseParameters(value, BasicHeaderValueParser.INSTANCE)

          val filtered = nameValuePairs.filter { nvp =>
            cookieNames.contains(nvp.getName)
          }

          Some(filtered)
        case _ => None
      }
    }.flatten

    cookies.map { cookie =>
      (("schema" -> "iglu:org.ietf/http_cookie/jsonschema/1-0-0") ~
        ("data" ->
          ("name"    -> cookie.getName) ~
            ("value" -> cookie.getValue)))
    }
  }
}
