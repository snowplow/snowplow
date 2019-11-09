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

import cats.data.ValidatedNel
import cats.syntax.either._

import com.snowplowanalytics.iglu.core.{SchemaCriterion, SchemaKey, SchemaVer, SelfDescribingData}

import io.circe._
import io.circe.syntax._

import utils.CirceUtils

object HttpHeaderExtractorEnrichment extends ParseableEnrichment {
  override val supportedSchema =
    SchemaCriterion(
      "com.snowplowanalytics.snowplow.enrichments",
      "http_header_extractor_config",
      "jsonschema",
      1,
      0
    )
  val outputSchema = SchemaKey("org.ietf", "http_header", "jsonschema", SchemaVer.Full(1, 0, 0))

  /**
   * Creates a HttpHeaderExtractorConf from a Json.
   * @param config The header_extractor enrichment JSON
   * @param schemaKey provided for the enrichment, must be supported by this enrichment
   * @return a HeaderExtractor configuration
   */
  override def parse(
    config: Json,
    schemaKey: SchemaKey,
    localMode: Boolean = false
  ): ValidatedNel[String, HttpHeaderExtractorConf] =
    (for {
      _ <- isParseable(config, schemaKey)
      headersPattern <- CirceUtils.extract[String](config, "parameters", "headersPattern").toEither
    } yield HttpHeaderExtractorConf(headersPattern)).toValidatedNel
}

/**
 * Enrichment extracting certain headers from headers.
 * @param headersPattern Names of the headers to be extracted
 */
final case class HttpHeaderExtractorEnrichment(headersPattern: String) extends Enrichment {
  case class Header(name: String, value: String)

  def extract(headers: List[String]): List[SelfDescribingData[Json]] = {
    val httpHeaders = headers.flatMap { header =>
      header.split(":", 2) match {
        case Array(name, value) if name.matches(headersPattern) =>
          Some(Header(name, value))
        case _ => None
      }
    }

    httpHeaders.map { header =>
      SelfDescribingData(
        HttpHeaderExtractorEnrichment.outputSchema,
        Json.obj(
          "name" := Json.fromString(header.name.trim),
          "value" := Json.fromString(header.value.trim)
        )
      )
    }
  }
}
