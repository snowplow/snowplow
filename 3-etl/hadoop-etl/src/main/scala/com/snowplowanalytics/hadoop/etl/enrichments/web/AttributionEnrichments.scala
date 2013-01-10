/*
 * Copyright (c) 2012 SnowPlow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.hadoop.etl
package enrichments
package web

// Java
import java.net.URI

// Scala
import scala.collection.JavaConversions._

// Scalaz
import scalaz._
import Scalaz._

// Apache URLEncodedUtils
import org.apache.http.NameValuePair
import org.apache.http.client.utils.URLEncodedUtils

// This project
import utils.EtlUtils

/**
 * Holds enrichments related to marketing and campaign
 * attribution.
 */
object AttributionEnrichments {

  /**
   * Immutable case class for a marketing campaign. Any
   * or all of the five fields can be set.
   */
  case class MarketingCampaign(
      val medium:   Option[String],
      val source:   Option[String],
      val term:     Option[String],
      val content:  Option[String],
      val campaign: Option[String])

  /**
   * Extract the marketing fields
   * from a URL.
   *
   * @param uri The URI to extract
   *        marketing fields from
   * @param encoding The encoding of
   *        the URI being parsed
   * @return the MarketingCampaign
   *         or an error message,
   *         boxed in a Scalaz
   *         Validation
   */
  def extractMarketingFields(uri: URI, encoding: String): Validation[String, MarketingCampaign] = {

    val parameters = try {
      Option(URLEncodedUtils parse(uri, encoding))
    } catch {
      case _ => return "Could not parse uri [%s]".format(uri).fail
    }

    // If somebody wants to rewrite this without
    // the mutable variables, please go ahead
    var medium, source, term, content, campaign: Option[String] = None
    for (params <- parameters) {
      for (p <- params.toList) {
        val name  = p.getName
        lazy val value = EtlUtils.decodeSafely(
          p.getValue.toLowerCase, encoding) // Should actually be lower case anyway

        name match {
          case "utm_medium" => medium = value
          case "utm_source" => source = value
          case "utm_term" => term = value
          case "utm_content" => content = value
          case "utm_campaign" => campaign = value
        }
      }
    }

    MarketingCampaign(
      medium,
      source,
      term,
      content,
      campaign).success
  }
}