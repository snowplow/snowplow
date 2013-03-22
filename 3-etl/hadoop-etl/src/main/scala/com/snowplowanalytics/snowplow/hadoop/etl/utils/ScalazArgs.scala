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
package com.snowplowanalytics.snowplow.hadoop.etl
package utils

// Scalaz
import scalaz._
import Scalaz._

// Scalding
import com.twitter.scalding.Args

/**
 * Module to hold the pimped class
 */
object ScalazArgs {

  /**
   * Implicit to pimp a Scalding
   * Args class to our Scalaz
   * Validation friendly version.
   *
   * @param args A Scalding Args
   *        object
   * @return the pimped ScalazArgs
   */
  implicit def pimpArgs(args: Args) = new ScalazArgs(args)
}

/**
 * The Scalding Args class pimped
 * with Scalaz Validation.
 *
 * Allows for better validation
 * handling which can be composed.
 */
class ScalazArgs(args: Args) {

  /**
   * A re-implementation of the
   * required() method, wrapped
   * in a Scalaz Validation.
   *
   * Use it to compose validation
   * errors if a key is missing
   * or set multiple times.
   *
   * @param key The name of the
   *        argument to retrieve
   * @return either the argument's
   *         value or an error,
   *         message, boxed in a
   *         Scalaz Validation
   */
  def requiredz(key: String): Validation[String, String] = try {
      args.optional(key) match {
        case Some(value) => value.success
        case None => "Required argument [%s] not found".format(key).fail
      }
    // TODO: clean this up. What is the specific Exception being thrown here?
    } catch {
      case _ => "List of values found for argument [%s], should be one".format(key).fail
    }
}