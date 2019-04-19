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
import com.snowplowanalytics.iglu.core.{SchemaCriterion, SchemaKey}
import io.circe._
import io.circe.syntax._
import org.apache.http.message.BasicHeaderValueParser

import utils.CirceUtils

object CookieExtractorEnrichment extends ParseableEnrichment {
  override val supportedSchema =
    SchemaCriterion("com.snowplowanalytics.snowplow", "cookie_extractor_config", "jsonschema", 1, 0)

  /**
   * Creates a CookieExtractorConf instance from a Json.
   * @param config The cookie_extractor enrichment JSON
   * @param schemaKey provided for the enrichment, must be supported by this enrichment
   * @return a CookieExtractor configuration
   */
  override def parse(
    config: Json,
    schemaKey: SchemaKey,
    localMode: Boolean = false
  ): ValidatedNel[String, CookieExtractorConf] =
    (for {
      _ <- isParseable(config, schemaKey)
      cookieNames <- CirceUtils.extract[List[String]](config, "parameters", "cookies").toEither
    } yield CookieExtractorConf(cookieNames)).toValidatedNel
}

/**
 * Enrichment extracting certain cookies from headers.
 * @param cookieNames Names of the cookies to be extracted
 */
final case class CookieExtractorEnrichment(cookieNames: List[String]) extends Enrichment {

  def extract(headers: List[String]): List[Json] = {
    // rfc6265 - sections 4.2.1 and 4.2.2
    val cookies = headers.flatMap { header =>
      header.split(":", 2) match {
        case Array("Cookie", value) =>
          val nameValuePairs =
            BasicHeaderValueParser.parseParameters(value, BasicHeaderValueParser.INSTANCE)

          val filtered = nameValuePairs.filter { nvp =>
            cookieNames.contains(nvp.getName)
          }

          Some(filtered)
        case _ => None
      }
    }.flatten

    cookies.map { cookie =>
      Json.obj(
        "schema" := "iglu:org.ietf/http_cookie/jsonschema/1-0-0",
        "data" := Json.obj(
          "name" := stringToJson(cookie.getName),
          "value" := stringToJson(cookie.getValue)
        )
      )
    }
  }

  private def stringToJson(str: String): Json =
    Option(str).map(Json.fromString).getOrElse(Json.Null)
}
