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
package com.snowplowanalytics.snowplow.enrich

// Java
import java.lang.{Integer => JInteger}

// Scalaz
import scalaz._
import Scalaz._

// Apache URLEncodedUtils
import org.apache.http.NameValuePair

// Scala MaxMind GeoIP
import com.snowplowanalytics.maxmind.geoip.IpLocation

// This project
import hadoop.inputs.CanonicalInput
import hadoop.outputs.CanonicalOutput

/**
 * Scala package object to hold types,
 * helper methods etc.
 *
 * See:
 * http://www.artima.com/scalazine/articles/package_objects.html
 */
package object hadoop {

  /**
   * Capture a client's
   * screen resolution
   */
  type ViewDimensionsTuple = (JInteger, JInteger) // Height, width.

  /**
   * Type alias for HTTP headers
   */
  type HttpHeaders = List[String]

  /**
   * Type alias for a non-empty
   * set of name-value pairs
   */
  type NameValueNel = NonEmptyList[NameValuePair]

  /**
   * Type alias for a `ValidationNel`
   * containing Strings for `Failure`
   * or any type of `Success`.
   *
   * @tparam A the type of `Success`
   */
  type Validated[A] = ValidationNel[String, A]

  /**
   * Type alias for a `Validation`
   * containing either error `String`s
   * or a `NameValueNel`.
   */
  type ValidatedNameValueNel = Validation[String, NameValueNel] // Note not Validated[]

  /**
   * Type alias for an `Option`-boxed String
   */
  type MaybeString = Option[String]

  /**
   * Type alias for an `Option`-boxed
   * `CanonicalInput`.
   */
  type MaybeCanonicalInput = Option[CanonicalInput]

  /**
   * Type alias for either a `ValidationNel`
   * containing `String`s for `Failure`
   * or a `MaybeCanonicalInput` for `Success`.
   */
  type ValidatedMaybeCanonicalInput = Validated[MaybeCanonicalInput]

  /**
   * Type alias for an `Option`-boxed
   * `CanonicalOutput`.
   */
  type MaybeCanonicalOutput = Option[CanonicalOutput]

  /**
   * Type alias for an `Option`-boxed
   * `IpLocation`.
   */
  type MaybeIpLocation = Option[IpLocation]

  /**
   * Type alias for either a `ValidationNel`
   * containing `String`s for `Failure`
   * or a CanonicalOutput for `Success`.
   */
  type ValidatedCanonicalOutput = Validated[CanonicalOutput]

  /**
   * Type alias for either a `ValidationNel`
   * containing `String`s for `Failure`
   * or a MaybeCanonicalOutput for `Success`.
   */
  type ValidatedMaybeCanonicalOutput = Validated[MaybeCanonicalOutput]
}
