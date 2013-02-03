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
package com.snowplowanalytics.snowplow.hadoop

// Scalaz
import scalaz._
import Scalaz._

// Apache URLEncodedUtils
import org.apache.http.NameValuePair

// This project
import etl.inputs.CanonicalInput
import etl.outputs.CanonicalOutput

/**
 * Scala package object to hold types,
 * helper methods etc.
 *
 * See:
 * http://www.artima.com/scalazine/articles/package_objects.html
 */
package object etl {

  /**
   * Because analysis is always easier
   * when date and time are in
   * separate fields.
   */
  type DateTimeTuple = (String, String)

  /**
   * Capture a client's
   * screen resolution
   */
  type ResolutionTuple = (Int, Int) // Height, width

  /**
   * Type alias for HTTP headers
   */
  type HttpHeaders = List[String]

  /**
   * Type alias for a non-empty
   * set of name-value pairs
   */
  type NameValueNEL = NonEmptyList[NameValuePair]

  /**
   * Type alias for a `ValidationNEL`
   * containing Strings for `Failure`
   * or any type of `Success`.
   *
   * @tparam A the type of `Success`
   */
  type Validated[A] = ValidationNEL[String, A]

  /**
   * Type alias for a `Validation`
   * containing either error `String`s
   * or a `NameValueNEL`.
   */
  type ValidatedNameValueNEL = Validation[String, NameValueNEL] // Note not Validated[]

  /**
   * Type alias for an `Option`-boxed
   * `CanonicalInput`.
   */
  type MaybeCanonicalInput2 = Option[CanonicalInput]

  /**
   * Type alias for either a `ValidationNEL`
   * containing `String`s for `Failure`
   * or a `MaybeCanonicalInput` for `Success`.
   */
  type ValidatedCanonicalInput = Validated[MaybeCanonicalInput2]

  /**
   * Type alias for an `Option`-boxed
   * `CanonicalOutput`.
   */
  type MaybeCanonicalOutput2 = Option[CanonicalOutput]

  /**
   * Type alias for either a `ValidationNEL`
   * containing `String`s for `Failure`
   * or a MaybeCanonicalOutput for `Success`.
   */
  type ValidatedCanonicalOutput = Validated[MaybeCanonicalOutput2]

  /**
   * Type alias for a `PartialFunction`
   * to handle unexpected errors.
   * Can contain a `Throwable` or a
   * `MaybeUnexpectedError`, with
   * `Success` of any type.
   *
   * @tparam A the type of `Success`
   *         within the ValidationNEL
   */
  // type UnexpectedErrorHandler[A] = PartialFunction[Throwable, Validated[A]]
}
