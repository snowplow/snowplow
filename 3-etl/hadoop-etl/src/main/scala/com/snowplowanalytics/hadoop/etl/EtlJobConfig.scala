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

/**
 * The configuration for the
 * SnowPlowEtlJob.
 */
case class EtlJobConfig(
    val inFolder: String,
    val outFolder: String)


    // val collectorLoader: CollectorLoader,
    // val continueOnUnexpectedError: Boolean)



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
   * @return the configuration, in a
   *         case class
   */
  def loadConfigFrom(args: Args): ValidationNEL[String, EtlJobConfig] = {

    // val in  = requiredz(args, "CLOUDFRONT_LOGS")
    // val out = requiredz(args, "EVENTS_TABLE")
    
    val iny: Validation[String, String] = "yo".fail
    val outy: Validation[String, String] = "yo".fail
    // val loader = requiredf(args, "COLLECTOR_FORMAT", (cf => CollectorLoader.getLoader(cf)))
    
    /*
    val continue = {
      val c = args required "CONTINUE_ON"
      c == "1"
    }
    
    val loader = {
      val cf = args required "COLLECTOR_FORMAT"
      CollectorLoader.getLoader(cf) getOrElse {
        throw new FatalValidationException("collector_format '%s' not supported" format cf)
      }
    } */

    (iny.liftFailNel ⊛ outy.liftFailNel) { EtlJobConfig(_, _) }
  }

  /**
   * Scalding's Args.required() method
   * given a Scalaz Validation wrapper
   *
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

  /**
   * Scalding's Args.required() method
   * given a Scalaz Validation wrapper
   *
   * TODO rest of description
   */
  private def requiredf[A](args: Args, key: String, f: (String => Validation[String, A])): Validation[String, A] = try {
      args.optional(key) match {
        case Some(value) => f(value)
        case None => "Required argument [%s] not found".format(key).fail
      }
    } catch {
      case _ => "List of values for argument [%s], should be one".format(key).fail
    }   
}