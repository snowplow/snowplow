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
import loaders.CollectorLoader
import utils.EtlUtils

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
   *         in a Scalaz Validation
   */
  def loadConfigFrom(args: Args): ValidationNEL[String, EtlJobConfig] = {

    val in  = args.requiredz("CLOUDFRONT_LOGS")
    val out = args.requiredz("EVENTS_TABLE")
    val loader = args.requiredz("COLLECTOR_FORMAT") flatMap (cf => CollectorLoader.getLoader(cf))
    val continue = args.requiredz("CONTINUE_ON") flatMap (co => EtlUtils.stringToBoolean(co))

    (in.toValidationNEL ⊛ out.toValidationNEL ⊛ loader.toValidationNEL ⊛ continue.toValidationNEL) { EtlJobConfig(_, _, _, _) }
  }

  /**
   * Scalding's Args.required() method,
   * wrapped with a Scalaz Validation.
   *
   * Use this to compose error messages
   * relating to 
   * TODO rest of description
   */
  private def requiredz(args: Args, key: String): Validation[String, String] = try {
      args.optional(key) match {
        case Some(value) => value.success
        case None => "Required argument [%s] not found".format(key).fail
      }
    } catch {
      case _ => "List of values for argument [%s], should be one".format(key).fail
    }
}