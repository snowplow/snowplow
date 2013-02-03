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

// Scalding
import com.twitter.scalding.Args

// This project
import utils.{ConversionUtils, ScalazArgs}
import inputs.CollectorLoader
import outputs.CanonicalOutput

/**
 * The configuration for the
 * SnowPlowEtlJob.
 */
case class EtlJobConfig(
    inFolder: String,
    outFolder: String,
    errFolder: String,
    collectorLoader: CollectorLoader) //,
    // unexpectedErrorHandler: UnexpectedErrorHandler[CanonicalOutput])

/**
 * Module to handle configuration for
 * the SnowPlowEtlJob
 */
object EtlJobConfig {

  /**
   * Loads the Config from the Scalding
   * job's supplied Args.
   *
   * @param args The arguments to parse
   * @return the EtLJobConfig, or one or
   *         more error messages, boxed
   *         in a Scalaz Validation NEL
   */
  def loadConfigFrom(args: Args): ValidationNEL[String, EtlJobConfig] = {

    import ScalazArgs._
    val inFolder  = args.requiredz("INPUT_FOLDER")
    val outFolder = args.requiredz("OUTPUT_FOLDER")
    val errFolder = args.requiredz("ERRORS_FOLDER")

    // TODO: change loader to be the whole end-to-end load
    val loader = args.requiredz("INPUT_FORMAT") flatMap (cf => CollectorLoader.getLoader(cf))
    // val outFormat TODO: add this

    /* val unexpectedErrorHandler = for {
      a <- args.requiredz("CONTINUE_ON")
      c <- ConversionUtils.stringToBoolean(a)
      hr = EtlJobFlow.buildUnexpectedErrorHandler(c)
    } yield hr */

    (inFolder.toValidationNEL |@| outFolder.toValidationNEL |@| errFolder.toValidationNEL |@| loader.toValidationNEL /*|@| unexpectedErrorHandler.toValidationNEL */) { EtlJobConfig(_, _, _, _/*, _*/) }
  }
}