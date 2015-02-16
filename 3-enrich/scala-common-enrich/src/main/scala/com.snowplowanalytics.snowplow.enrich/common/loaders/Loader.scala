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
package loaders

// Java
import java.net.URI

// Apache URLEncodedUtils
import org.apache.http.client.utils.URLEncodedUtils

// Scala
import scala.collection.JavaConversions._

// Scalaz
import scalaz._
import Scalaz._

/**
 * Companion object to the CollectorLoader.
 * Contains factory methods.
 */
object Loader {

  private val TsvRegex = "^tsv/(.*)$".r

  /**
   * Factory to return a CollectorLoader
   * based on the supplied collector
   * identifier (e.g. "cloudfront" or
   * "clj-tomcat").
   *
   * @param collector Identifier for the
   *        event collector
   * @return a CollectorLoader object, or
   *         an an error message, boxed
   *         in a Scalaz Validation
   */
  def getLoader(collectorOrProtocol: String): Validation[String, Loader[_]] = collectorOrProtocol match {
    case "cloudfront" => CloudfrontLoader.success
    case "clj-tomcat" => CljTomcatLoader.success
    case "thrift"     => ThriftLoader.success // Finally - a data protocol rather than a piece of software
    case TsvRegex(f)  => TsvLoader(f).success
    case  c           => "[%s] is not a recognised Snowplow event collector".format(c).fail
  }
}

/**
 * All loaders must implement this
 * abstract base class.
 */
abstract class Loader[T] {
  
  import CollectorPayload._

  /**
   * Converts the source string into a 
   * CanonicalInput.
   *
   * TODO: need to change this to
   * handling some sort of validation
   * object.
   *
   * @param line A line of data to convert
   * @return a CanonicalInput object, Option-
   *         boxed, or None if no input was
   *         extractable.
   */
  def toCollectorPayload(line: T): ValidatedMaybeCollectorPayload

  /**
   * Converts a querystring String
   * into a non-empty list of NameValuePairs.
   *
   * Returns a non-empty list of 
   * NameValuePairs on Success, or a Failure
   * String.
   *
   * @param qs Option-boxed querystring
   *        String to extract name-value
   *        pairs from, or None
   * @param encoding The encoding used
   *        by this querystring
   * @return either a NonEmptyList of
   *         NameValuePairs or an error
   *         message, boxed in a Scalaz
   *         Validation
   */
  protected[loaders] def parseQuerystring(qs: Option[String], enc: String): ValidatedNameValuePairs = qs match {
    case Some(q) => {
      try {
        URLEncodedUtils.parse(URI.create("http://localhost/?" + q), enc).toList.success
      } catch {
        case e => "Exception extracting name-value pairs from querystring [%s] with encoding [%s]: [%s]".format(q, enc, e.getMessage).fail
      }
    }
    case None => Nil.success
  }
}