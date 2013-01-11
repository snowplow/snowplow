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

// Scalaz
import scalaz._
import Scalaz._

// Scalding
import com.twitter.scalding.Args

// This project
import inputs.CollectorLoader
import utils.{EtlUtils, ScalazArgs}

/**
 * The configuration for the
 * SnowPlowEtlJob.
 */
case class EtlJobConfig(
    val inFolder: String,
    val outFolder: String,
    val collectorLoader: CollectorLoader,
    val continueOnUnexpectedError: Boolean)

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
    val loader = args.requiredz("INPUT_FORMAT") flatMap (cf => CollectorLoader.getLoader(cf))
    val outFolder = args.requiredz("OUTPUT_FOLDER")
    // TODO: add in output format to    
    val continue = args.requiredz("CONTINUE_ON") flatMap (co => EtlUtils.stringToBoolean(co))

    (inFolder.toValidationNEL |@| outFolder.toValidationNEL |@| loader.toValidationNEL |@| continue.toValidationNEL) { EtlJobConfig(_, _, _, _) }
  }
}