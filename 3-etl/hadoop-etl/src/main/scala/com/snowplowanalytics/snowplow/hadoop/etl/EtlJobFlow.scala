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

// Scalaz
import scalaz._
import Scalaz._

// This project
import enrichments.EnrichmentManager

/**
 * Holds constructs to help
 * build the ETL job's data
 * flow.
 */ 
object EtlJobFlow {

  /**
   * A helper method to take a ValidatedCanonicalOutput
   * and flatMap it into a ValidatedCanonicalOutput.
   *
   * We have to do some unboxing because enrichEvent
   * expects a raw CanonicalInput as its argument, not
   * a MaybeCanonicalInput.
   *
   * @param input The ValidatedCanonicalInput
   * @return the ValidatedCanonicalOutput. Thanks to
   *         flatMap, will include any validation errors
   *         contained within the ValidatedCanonicalInput
   */
  def toCanonicalOutput(input: ValidatedCanonicalInput): ValidatedCanonicalOutput = {
    input.flatMap {
      _.cata(EnrichmentManager.enrichEvent(_).map(_.some),
             none.success)
    }
  }

  /**
   * A factory to build the handler
   * for unexpected exceptions.
   *
   * @param continueOn Whether to
   *        continue on unexpected
   *        error, or throw an
   *        exception
   * @return a catch block i.e. a
   *         PartialFunction boxing
   *         either a Throwable or a
   *         ValidationNEL. See the
   *         package object for
   *         details of the type
   */
  // def buildUnexpectedErrorHandler(continueOn: Boolean): UnexpectedErrorHandler[CanonicalOutput] = {
  //   case e if continueOn => "Unexpected error occurred: [%s] (continuing...)".format(e.getMessage).failNel[CanonicalOutput]
  //  case e => throw e
  //}
}