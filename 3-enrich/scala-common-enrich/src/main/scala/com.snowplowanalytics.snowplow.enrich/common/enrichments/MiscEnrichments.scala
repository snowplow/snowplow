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
package com.snowplowanalytics.snowplow.enrich.common
package enrichments

// Scalaz
import scalaz._
import Scalaz._

// json4s
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

// This project
import utils.{ConversionUtils => CU}

// Get our project settings
import generated.ProjectSettings

/**
 * Miscellaneous enrichments which don't fit into
 * one of the other modules.
 */
object MiscEnrichments {

  /**
   * The version of this ETL. Appends this version
   * to the supplied "host" ETL.
   *
   * @param hostEtlVersion The version of the host ETL
   *        running this library
   * @return the complete ETL version
   */
  def etlVersion(hostEtlVersion: String): String =
    "%s-common-%s".format(hostEtlVersion, ProjectSettings.version)

  /**
   * Validate the specified
   * platform.
   *
   * @param field The name of
   *        the field being
   *        processed
   * @param platform The code
   *        for the platform
   *        generating this
   *        event.
   * @return a Scalaz
   *         ValidatedString.
   */
  val extractPlatform: (String, String) => ValidatedString = (field, platform) => {
    platform match {
      case "web"  => "web".success // Web, including Mobile Web
      case "iot"  => "iot".success // Internet of Things (e.g. Arduino tracker)
      case "app"  => "app".success // General App
      case "mob"  => "mob".success // Mobile / Tablet
      case "pc"   => "pc".success // Desktop / Laptop / Netbook
      case "cnsl" => "cnsl".success // Games Console
      case "tv"   => "tv".success // Connected TV
      case "srv"  => "srv".success // Server-side App
      case p      => "Field [%s]: [%s] is not a supported tracking platform".format(field, p).fail
    }
  }

  /**
   * Identity transform.
   * Straight passthrough.
   */
  val identity: (String, String) => ValidatedString = (field, value) => value.success

  /**
   * Make a String TSV safe
   */
  val toTsvSafe: (String, String) => ValidatedString = (field, value) => CU.makeTsvSafe(value).success

  /**
   * The X-Forwarded-For header can contain a comma-separated list of IPs especially if it has
   * gone through multiple load balancers.
   * Here we retrieve the first one as it is supposed to be the client one, c.f.
   * https://en.m.wikipedia.org/wiki/X-Forwarded-For#Format
   */
  val extractIp: (String, String) => ValidatedString = (field, value) => {
    val lastIp = Option(value).map(_.split("[,|, ]").head).orNull
    CU.makeTsvSafe(lastIp).success
  }

  /**
   * Turn a list of custom contexts into a self-describing JSON
   *
   * @param derivedContexts
   * @return Self-describing JSON of custom contexts
   */
  def formatDerivedContexts(derivedContexts: List[JObject]): String =
    compact(
      render(
        ("schema" -> "iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-1") ~
          ("data" -> JArray(derivedContexts))
      ))
}
