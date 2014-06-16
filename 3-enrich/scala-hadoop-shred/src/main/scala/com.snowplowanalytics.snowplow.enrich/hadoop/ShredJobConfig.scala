/*
 * Copyright (c) 2014 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics
package snowplow
package enrich
package hadoop

// Jackson
import com.fasterxml.jackson.databind.JsonNode

// Scalaz
import scalaz._
import Scalaz._

// Scalding
import com.twitter.scalding.Args

// Iglu Scala Client
import iglu.client.Resolver
import iglu.client.validation.ProcessingMessageMethods._

// This project
import utils.ScalazArgs

/**
 * The configuration for the SnowPlowEtlJob.
 */
case class ShredJobConfig(
    inFolder: String,
    outFolder: String,
    badFolder: String,
    exceptionsFolder: Option[String],
    resolver: Resolver)

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
  def loadConfigFrom(args: Args): ValidatedNel[ShredJobConfig] = {

    import ScalazArgs._
    val inFolder  = args.requiredz("input_folder").toProcessingMessageNel
    val outFolder = args.requiredz("output_folder").toProcessingMessageNel
    val badFolder = args.requiredz("bad_rows_folder").toProcessingMessageNel
    val exceptionsFolder = args.optionalz("exceptions_folder").toProcessingMessageNel
    val resolver  = args.requiredz("iglu_config").fold(
      e => e.toProcessingMessageNel.fail,
      s => base64ToJsonNode(s).flatMap(Resolver.parse(_))
    )

    (inFolder |@| outFolder |@| badFolder |@| exceptionsFolder |@| resolver) { ShredJobConfig(_,_,_,_,_) }
  }

  /**
   * Converts a base64-encoded JSON
   * String into a JsonNode.
   *
   * @param str base64-encoded JSON
   * @return a JsonNode on Success,
   *         a NonEmptyList of
   *         ProcessingMessages on
   *         Failure 
   */
  private def base64ToJsonNode(str: String): ValidatedNel[JsonNode] = {
    // TODO: implement this
    "TODO".toProcessingMessageNel.fail
  }
}
