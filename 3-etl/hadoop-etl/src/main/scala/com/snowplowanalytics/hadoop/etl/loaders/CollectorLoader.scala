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
package loaders

/**
 * Companion object to the CollectorLoader.
 * Contains factory methods.
 */
object CollectorLoader {
  
  /**
   * Factory to return a CollectorLoader
   * based on the supplied collector
   * identifier.
   *
   * @param collector Identifier for the collector
   * @return a CollectorLoader object,
   *         Option-boxed, or None if `collector`
   *         was not recognised
   */
  def getLoader(collector: String): Option[CollectorLoader] = collector match {
    case "cloudfront" => Some(CloudFrontLoader)
    case "clj-tomcat" => Some(CloudFrontLoader)
    case _            => None
  }
}

/**
 * All loaders must implement this
 * abstract base class.
 */
abstract class CollectorLoader {
  
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
  def toCanonicalInput(line: String): Option[CanonicalInput]

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