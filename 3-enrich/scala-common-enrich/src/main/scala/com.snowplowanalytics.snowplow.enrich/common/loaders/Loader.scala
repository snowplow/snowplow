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
package loaders

import java.net.URI
import java.nio.charset.Charset

import scala.collection.JavaConverters._

import cats.data.ValidatedNel
import cats.syntax.either._
import cats.syntax.option._
import org.apache.http.NameValuePair
import org.apache.http.client.utils.URLEncodedUtils
import org.joda.time.DateTime

import outputs._
import utils.JsonUtils

/** Companion object to the CollectorLoader. Contains factory methods. */
object Loader {
  private val TsvRegex = "^tsv/(.*)$".r
  private val NdjsonRegex = "^ndjson/(.*)$".r

  /**
   * Factory to return a CollectorLoader based on the supplied collector identifier (e.g.
   * "cloudfront" or "clj-tomcat").
   * @param collector Identifier for the event collector
   * @return either a CollectorLoader object or an an error message
   */
  def getLoader(collectorOrProtocol: String): Either[String, Loader[_]] =
    collectorOrProtocol match {
      case "cloudfront" => CloudfrontLoader.asRight
      case "clj-tomcat" => CljTomcatLoader.asRight
      // a data protocol rather than a piece of software
      case "thrift" => ThriftLoader.asRight
      case TsvRegex(f) => TsvLoader(f).asRight
      case NdjsonRegex(f) => NdjsonLoader(f).asRight
      case c => s"[$c] is not a recognised Snowplow event collector".asLeft
    }
}

/** All loaders must implement this abstract base class. */
abstract class Loader[T] {

  /**
   * Converts the source string into a CanonicalInput.
   * TODO: need to change this to handling some sort of validation object.
   * @param line A line of data to convert
   * @return a CanonicalInput object, Option-boxed, or None if no input was extractable.
   */
  def toCollectorPayload(line: T): ValidatedNel[CPFormatViolationMessage, Option[CollectorPayload]]

  /**
   * Converts a querystring String into a non-empty list of NameValuePairs.
   * Returns a non-empty list of NameValuePairs on Success, or a Failure String.
   * @param qs Option-boxed querystring String to extract name-value pairs from, or None
   * @param encoding The encoding used by this querystring
   * @return either a NEL of NameValuePairs or an error message
   */
  protected[loaders] def parseQuerystring(
    qs: Option[String],
    enc: Charset
  ): Either[CPFormatViolationMessage, List[NameValuePair]] = qs match {
    case Some(q) =>
      Either
        .catchNonFatal(URLEncodedUtils.parse(URI.create("http://localhost/?" + q), enc))
        .map(_.asScala.toList)
        .leftMap { e =>
          val msg = s"could not extract name-value pairs from querystring with encoding $enc: " +
            JsonUtils.stripInstanceEtc(e.getMessage).orNull
          InputDataCPFormatViolationMessage("querystring", qs, msg)
        }
    case None => Nil.asRight
  }

  /**
   * Converts a CloudFront log-format date and a time to a timestamp.
   * @param date The CloudFront log-format date
   * @param time The CloudFront log-format time
   * @return either the timestamp as a Joda DateTime or an error String
   */
  protected[loaders] def toTimestamp(
    date: String,
    time: String
  ): Either[CPFormatViolationMessage, DateTime] =
    Either
      .catchNonFatal(DateTime.parse("%sT%s+00:00".format(date, time)))
      .leftMap { e =>
        val msg = s"could not convert timestamp: ${e.getMessage}"
        InputDataCPFormatViolationMessage("dateTime", s"$date $time".some, msg)
      }

  /**
   * Checks whether a String field is a hyphen "-", which is used by CloudFront to signal a null.
   * @param field The field to check
   * @return True if the String was a hyphen "-"
   */
  private[loaders] def toOption(field: String): Option[String] = Option(field) match {
    case Some("-") => None
    case Some("") => None
    case s => s // Leaves any other Some(x) or None as-is
  }
}
