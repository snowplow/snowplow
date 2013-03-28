/*
 * Copyright (c) 2012-2013 SnowPlow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.enrich.hadoop
package enrichments
package web

// Java
import java.net.URI

// Scalaz
import scalaz._
import Scalaz._

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
   * variable, depending on some
   * business rules: see also
   * `choosePageUri` below.
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
      fromTracker: Option[String]): Validation[String, URI] = {

    (fromReferer, fromTracker) match {
      case (Some(r), None)    => toUri(r)
      case (None, Some(t))    => toUri(t)
      case (Some(r), Some(t)) => choosePageUri(r, t) flatMap (pu => toUri(pu))
      case (None, None)       => "No page URI provided".fail
    }
  }

  /**
   * Let's us choose between
   * the page URI from the
   * collector's referer and
   * the page URI as set in
   * the tracker, when both
   * are present.
   *
   * TODO: add a warning if
   * referer page URI is
   * shorter than tracker
   * page URI.
   *
   * @param fromReferer The
   *        page URI reported
   *        as the referer to
   *        the collector
   * @param fromTracker The
   *        page URI reported
   *        by the tracker
   * @return either the chosen
   *         page URI as a
   *         String, or an
   *         error, all wrapped
   *         in a Validation
   */
  private def choosePageUri(fromReferer: String, fromTracker: String): Validation[String, String] =
    try {
      if (fromReferer == fromTracker) fromTracker.success // 98% of the time
      else if (fromReferer.length > fromTracker.length) fromReferer.success // Page URL got truncated in the GET, use referer URI
      else fromTracker.success // Corruption in the collector log? TODO: add a warning when we support warnings
    } catch {
      case e => "Unexpected error choosing page URI from [%s] and [%s]: [%s]".format(fromReferer, fromTracker, e.getMessage).fail
    }

  /**
   * A wrapper around Java's
   * URI.parse().
   *
   * Exceptions thrown by
   * URI.parse():
   * 1. NullPointerException
   *    if uri is null
   * 2. IllegalArgumentException
   *    if uri violates RFC 2396
   *
   * @param uri The URI string to
   *        convert
   * @return the URI object, or an
   *         error message, all
   *         wrapped in a Validation
   */       
  private def toUri(uri: String): Validation[String, URI] =
    try {
      URI.create(uri).success
    } catch {
      case e: NullPointerException => "Provided URI string was null".fail
      case e: IllegalArgumentException => "Provided URI string [%s] violates RFC 2396: [%s]".format(uri, e.getMessage).fail
      case e => "Unexpected error creating URI from string [%s]: [%s]".format(uri, e.getMessage).fail
    }
}