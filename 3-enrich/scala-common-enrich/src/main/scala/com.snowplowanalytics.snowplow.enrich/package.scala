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
package com.snowplowanalytics.snowplow.enrich

// Java
import java.lang.{Integer => JInteger}

// Scalaz
import scalaz._
import Scalaz._

// Apache URLEncodedUtils
import org.apache.http.NameValuePair

// Scala MaxMind GeoIP
import com.snowplowanalytics.maxmind.iplookups.IpLocation

// JSON Schema
import com.github.fge.jsonschema.core.report.ProcessingMessage

// This project
import common.loaders.CollectorPayload
import common.adapters.RawEvent
import common.outputs.EnrichedEvent
import common.enrichments.registry.Enrichment

/**
 * Scala package object to hold types,
 * helper methods etc.
 *
 * See:
 * http://www.artima.com/scalazine/articles/package_objects.html
 */
package object common {

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
   * Type alias for a map whose
   * keys are enrichment names and
   * whose values are enrichments
   */
  type EnrichmentMap = Map[String, Enrichment]

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
   * containing either an error
   * `String` or a success `String`.
   */
  type ValidatedString = Validation[String, String]

  /**
   * Type alias for a `Validation`
   * containing either error `String`s
   * or a `NameValueNel`.
   */
  type ValidatedNameValuePairs = Validation[String, List[NameValuePair]] // Note not Validated[]

  /**
   * Type alias for an `Option`-boxed String
   */
  type MaybeString = Option[String]

  /**
   * Type alias for an `Option`-boxed
   * `CollectorPayload`.
   */
  type MaybeCollectorPayload = Option[CollectorPayload]

  /**
   * Type alias for either a `ValidationNel`
   * containing `String`s for `Failure`
   * or a `MaybeCanonicalInput` for `Success`.
   */
  type ValidatedMaybeCollectorPayload = Validated[MaybeCollectorPayload]

  /**
   * Type alias for either a `ValidationNel`
   * containing `String`s for `Failure`
   * or a `List` of `RawEvent`s for `Success`.
   */
  type ValidatedRawEvents = Validated[NonEmptyList[RawEvent]]

  /**
   * Type alias for an `Option`-boxed
   * `CanonicalOutput`.
   */
  type MaybeEnrichedEvent = Option[EnrichedEvent]

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
  type ValidatedEnrichedEvent = Validated[EnrichedEvent]

  /**
   * Type alias for a `Validation`
   * containing ProcessingMessages
   * for `Failure` or any type for
   * `Success`
   *
   * @tparam A the type of `Success`
   */
  type ValidatedMessage[A] = Validation[ProcessingMessage, A]

  /**
   * Type alias for a `ValidationNel`
   * containing ProcessingMessage
   * for `Failure` or any type for
   * `Success`
   */
  type ValidatedNelMessage[A] = ValidationNel[ProcessingMessage, A]

  /**
   * Parameters inside of a raw event
   */
  type RawEventParameters = Map[String, String]
}
