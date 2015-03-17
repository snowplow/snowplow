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
package com.snowplowanalytics.snowplow.enrich.common
package enrichments
package web

// Java
import java.net.URI

// Scalaz
import scalaz._
import Scalaz._

// This project
import utils.{ConversionUtils => CU}

/**
 * Holds enrichments related to the
 * web page's URL, and the document
 * object contained within the page.
 */
object PageEnrichments {

  /**
   * Extracts the page URI from
   * either the collector's referer
   * or the appropriate tracker
   * variable. Tracker variable
   * takes precedence as per #268
   *
   * @param fromReferer The
   *        page URI reported
   *        as the referer to
   *        the collector
   * @param fromTracker The
   *        page URI reported
   *        by the tracker
   * @return either the chosen
   *         page URI, or an
   *         error, wrapped in a
   *         Validation
   */
  def extractPageUri(
      fromReferer: Option[String],
      fromTracker: Option[String]): Validation[String, Option[URI]] = {

    (fromReferer, fromTracker) match {
      case (Some(r), None)    => CU.stringToUri(r)
      case (None, Some(t))    => CU.stringToUri(t)
      case (Some(r), Some(t)) => CU.stringToUri(t) // Tracker URL takes precedence
      case (None, None)       => None.success // No page URI available. Not a failable offence
    }
  }

  /**
   * Extract the referrer domain user ID and timestamp from the "_sp={{DUID}}.{{TSTAMP}}"
   * portion of the querystring
   *
   * @param qsMap The querystring converted to a map
   * @return Validation boxing a pair of optional strings corresponding to the two fields
   */
  def parseCrossDomain(qsMap: Map[String, String]): Validation[String, (Option[String], Option[String])] = {
    qsMap.get("_sp") match {
      case Some("") => (None, None).success
      case Some(sp) => {
        val crossDomainElements = sp.split("\\.")

        val duid = CU.makeTsvSafe(crossDomainElements(0)).some
        val tstamp = crossDomainElements.lift(1) match {
          case Some(spDtm) => EventEnrichments.extractTimestamp("sp_dtm", spDtm).map(_.some)
          case None => None.success
        }

        tstamp.map(duid -> _)
      }
      case None => (None -> None).success
    }
  }
}
