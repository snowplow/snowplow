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
package enrichments

import cats.syntax.either._
import io.circe._
import io.circe.syntax._

import com.snowplowanalytics.snowplow.badrows.{FailureDetails, Processor}

import generated.ProjectSettings
import utils.{ConversionUtils => CU}

/** Miscellaneous enrichments which don't fit into one of the other modules. */
object MiscEnrichments {

  /**
   * The version of this ETL. Appends this version to the supplied "host" ETL.
   * @param processor The version of the host ETL running this library
   * @return the complete ETL version
   */
  def etlVersion(processor: Processor): String =
    s"${processor.artifact}-${processor.version}-common-${ProjectSettings.version}"

  /**
   * Validate the specified platform.
   * @param field The name of the field being processed
   * @param platform The code for the platform generating this event.
   * @return a Scalaz ValidatedString.
   */
  val extractPlatform: (String, String) => Either[FailureDetails.EnrichmentStageIssue, String] =
    (field, platform) =>
      platform match {
        case "web" => "web".asRight // Web, including Mobile Web
        case "iot" => "iot".asRight // Internet of Things (e.g. Arduino tracker)
        case "app" => "app".asRight // General App
        case "mob" => "mob".asRight // Mobile / Tablet
        case "pc" => "pc".asRight // Desktop / Laptop / Netbook
        case "cnsl" => "cnsl".asRight // Games Console
        case "tv" => "tv".asRight // Connected TV
        case "srv" => "srv".asRight // Server-side App
        case _ =>
          val msg = "not recognized as a tracking platform"
          val f = FailureDetails.EnrichmentFailureMessage.InputData(
            field,
            Option(platform),
            msg
          )
          FailureDetails.EnrichmentFailure(None, f).asLeft
      }

  /** Make a String TSV safe */
  val toTsvSafe: (String, String) => Either[FailureDetails.EnrichmentStageIssue, String] =
    (_, value) => CU.makeTsvSafe(value).asRight

  /**
   * The X-Forwarded-For header can contain a comma-separated list of IPs especially if it has
   * gone through multiple load balancers.
   * Here we retrieve the first one as it is supposed to be the client one, c.f.
   * https://en.m.wikipedia.org/wiki/X-Forwarded-For#Format
   */
  val extractIp: (String, String) => Either[FailureDetails.EnrichmentStageIssue, String] =
    (_, value) => {
      val lastIp = Option(value).map(_.split("[,|, ]").head).orNull
      CU.makeTsvSafe(lastIp).asRight
    }

  /**
   * Turn a list of custom contexts into a self-describing JSON
   * @param derivedContexts
   * @return Self-describing JSON of custom contexts
   */
  def formatDerivedContexts(derivedContexts: List[Json]): String =
    Json
      .obj(
        "schema" := "iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-1",
        "data" := Json.arr(derivedContexts: _*)
      )
      .noSpaces
}
