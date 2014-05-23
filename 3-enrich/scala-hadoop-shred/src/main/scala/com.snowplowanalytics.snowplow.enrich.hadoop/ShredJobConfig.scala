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
package hadoop

// Scalaz
import scalaz._
import Scalaz._

// Scalding
import com.twitter.scalding.Args

// This project
import utils.ScalazArgs

/**
 * The configuration for the SnowPlowEtlJob.
 */
case class ShredJobConfig(
    inFolder: String,
    outFolder: String,
    badFolder: String,
    exceptionsFolder: Option[String])

/**
 * Module to handle configuration for
 * the SnowPlowEtlJob
 */
object ShredJobConfig {

  /**
   * Loads the Config from the Scalding
   * job's supplied Args.
   *
   * @param args The arguments to parse
   * @return the EtLJobConfig, or one or
   *         more error messages, boxed
   *         in a Scalaz Validation Nel
   */
  def loadConfigFrom(args: Args): ValidationNel[String, ShredJobConfig] = {

    import ScalazArgs._
    val inFolder  = args.requiredz("input_folder")
    val outFolder = args.requiredz("output_folder")
    val badFolder = args.requiredz("bad_rows_folder")
    val exceptionsFolder = args.optionalz("exceptions_folder")
    
    (inFolder.toValidationNel |@| outFolder.toValidationNel |@| badFolder.toValidationNel |@| exceptionsFolder.toValidationNel) { ShredJobConfig(_,_,_,_) }
  }
}
