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
package inputs

// Scalaz
import scalaz._
import Scalaz._

/**
 * Companion object to the CollectorLoader.
 * Contains factory methods.
 */
object CollectorLoader {

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
  def getLoader(collectorOrProtocol: String): Validation[String, CollectorLoader[_]] = collectorOrProtocol match {
    case "cloudfront" => CloudfrontLoader.success
    case "clj-tomcat" => CljTomcatLoader.success
    case "thrift-raw" => ThriftLoader.success // Finally - a data protocol rather than a piece of software
    case  c           => "[%s] is not a recognised Snowplow event collector".format(c).fail
  }
}

/**
 * All loaders must implement this
 * abstract base class.
 */
abstract class CollectorLoader[T] {
  
  import CanonicalInput._

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
  def toCanonicalInput(line: T): ValidatedMaybeCanonicalInput

  /**
   * Checks whether a request to
   * a collector is a tracker
   * hitting the ice pixel.
   *
   * @param path The request path
   * @return true if this is a request
   *         for the ice pixel
   */
  protected def isIceRequest(path: String): Boolean =
    path.startsWith("/ice.png") || // Legacy name for /i
    path.equals("/i") ||
    path.startsWith("/i?")
}