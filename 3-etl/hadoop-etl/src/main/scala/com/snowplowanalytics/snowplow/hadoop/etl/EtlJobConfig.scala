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
 * The configuration for the SnowPlowEtlJob.
 */
case class EtlJobConfig(
    inFolder: String,
    outFolder: String,
    badFolder: String,
    collectorLoader: CollectorLoader)

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
   *         in a Scalaz Validation Nel
   */
  def loadConfigFrom(args: Args): ValidationNel[String, EtlJobConfig] = {

    import ScalazArgs._
    val inFolder  = args.requiredz("input_folder")
    val outFolder = args.requiredz("output_folder")
    val badFolder = args.requiredz("bad_rows_folder")

    // Don't instantiate till we're on the cluster nodes
    lazy val loader = args.requiredz("input_format") flatMap (cf => CollectorLoader.getLoader(cf))
    
    // TODO: add in support for CONTINUE_ON and the Failure Trap

    (inFolder.toValidationNel |@| outFolder.toValidationNel |@| badFolder.toValidationNel |@| loader.toValidationNel) { EtlJobConfig(_, _, _, _) }
  }
}