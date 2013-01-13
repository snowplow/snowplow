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
package inputs

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

// Joda-Time
import org.joda.time.DateTime

/**
 * The canonical input format for the ETL
 * process: it should be possible to
 * convert any collector input format to
 * this format, ready for the main,
 * collector-agnostic stage of the ETL.
 */
final case class CanonicalInput(
    val timestamp:  DateTime,
    val payload:    TrackerPayload, // See below for defn.
    val ipAddress:  Option[String],
    val userAgent:  Option[String],
    val refererUri: Option[String],
    val userId:     Option[String])

/**
 * A companion object which holds
 * factories to extract the
 * different possible payloads,
 * and related types.
 */
object TrackerPayload {

  /**
   * Converts a querystring String
   * into the GetPayload for SnowPlow:
   * a non-empty list of NameValuePairs.
   *
   * Returns a non-empty list of 
   * NameValuePairs, or an error.
   *
   * @param qs The querystring
   *        String to extract name-value
   *        pairs from
   * @param encoding The encoding used
   *        by this querystring
   * @return either a NonEmptyList of
   *         NameValuePairs or an error
   *         message, boxed in a Scalaz
   *         Validation
   */
  def extractGetPayload(qs: String, encoding: String): MaybeNameValueNEL[String] =
    try {
      parseQuerystring(qs, encoding) match {
        case head :: tail => NonEmptyList[NameValuePair](head, tail: _*).success
        case Nil => "No name-value pairs extractable from querystring [%s] with encoding [%s]".format(qs, encoding).fail
      }
    } catch {
      case e => "Exception extracting name-value pairs from querystring [%s] with encoding [%s]: [%s]".format(qs, encoding, e.getMessage).fail
    }

  /**
   * Helper to extract NameValuePairs.
   *
   * Health warning: does not handle any
   * exceptions from encoding errors.
   * Only call this from a method that
   * catches exceptions.
   *
   * @param qs The querystring
   *        String to extract name-value
   *        pairs from
   * @param encoding The encoding used
   *        by this querystring
   * @return the List of NameValuePairs
   */
  private def parseQuerystring(qs: String, encoding: String): List[NameValuePair] =
    URLEncodedUtils.parse(URI.create("http://localhost/?" + qs), encoding).toList
}

/**
 * All payloads sent by trackers must inherit from
 * this class.
 */
trait TrackerPayload

/**
 * A tracker payload for a single event, delivered
 * via the querystring on a GET.
 */
case class GetPayload(val payload: NameValueNEL) extends TrackerPayload