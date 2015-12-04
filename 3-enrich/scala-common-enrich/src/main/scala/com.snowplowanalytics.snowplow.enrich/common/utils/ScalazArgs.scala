/*
 * Copyright (c) 2014 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics
package snowplow
package enrich
package common
package utils

// Scala
import scala.language.reflectiveCalls
import scala.language.implicitConversions

// Scalaz
import scalaz._
import Scalaz._

// Iglu Scala Client
import iglu.client.validation.ProcessingMessageMethods._

/**
 * Module to hold the pimped class
 */
object ScalazArgs {

  /**
   * To avoid having Scalding as a dependency of Common Enrich,
   * we use this structural type in place of com.twitter.scalding.Args
   */
  type ListableArg = {
    def list(key: String): List[String]
  }

  /**
   * Implicit to pimp a Scalding
   * Args class to our Scalaz
   * Validation friendly version.
   *
   * @param args A Scalding Args
   * object
   * @return the pimped ScalazArgs
   */
  implicit def pimpArgs(args: ListableArg) = new ScalazArgs(args)
}

/**
 * The Scalding Args class pimped
 * with Scalaz Validation.
 *
 * Allows for better validation
 * handling which can be composed.
 */
class ScalazArgs(args: ScalazArgs.ListableArg) {

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
   * argument to retrieve
   * @return either the argument's
   * value or an error,
   * message, boxed in a
   * Scalaz Validation
   */
  def requiredz(key: String): ValidatedMessage[String] = args.list(key) match {
    case Nil => "Required argument [%s] not found".format(key).toProcessingMessage.fail[String]
    case List(a) => a.success
    case _ => "List of values found for argument [%s], should be one".format(key).toProcessingMessage.fail[String]
  }

  /**
   * A re-implementation of the optional()
   * method, wrapped in a Scalaz Validation.
   *
   * Use it to compose validation errors if
   * a key is missing.
   */
  def optionalz(key: String): ValidatedMessage[Option[String]] = args.list(key) match {
    case Nil => None.success
    case List(a) => Some(a).success
    case _ => "List of values found for argument [%s], should be at most one".format(key).toProcessingMessage.fail
  }

}
