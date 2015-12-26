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

// Snowplow Common Enrich
import common.utils.ConversionUtils

// This project
import utils.{
  ScalazArgs,
  JsonUtils
}

/**
 * The configuration for the SnowPlowEtlJob.
 */
case class ShredJobConfig(
    inFolder: String,
    outFolder: String,
    badFolder: String,
    exceptionsFolder: Option[String],
    igluResolver: Resolver)

/**
 * Module to handle configuration for
 * the SnowPlowEtlJob
 */
object ShredJobConfig {

  private val IgluConfigArg = "iglu_config"

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
    val inFolder  = args.requiredz("input_folder")
    val outFolder = args.requiredz("output_folder")
    val badFolder = args.requiredz("bad_rows_folder")
    val exceptionsFolder = args.optionalz("exceptions_folder")

    val igluResolver = args.requiredz(IgluConfigArg) match {
      case Failure(e) => e.failNel
      case Success(s) => for {
        node <- (base64ToJsonNode(s).toValidationNel: ValidatedNel[JsonNode])
        reso <- Resolver.parse(node)
      } yield reso
    }

    (inFolder.toValidationNel |@| outFolder.toValidationNel |@| badFolder.toValidationNel |@| exceptionsFolder.toValidationNel |@| igluResolver) { ShredJobConfig(_,_,_,_,_) }
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
  private[hadoop] def base64ToJsonNode(str: String): Validated[JsonNode] =
    (for {
      raw  <- ConversionUtils.decodeBase64Url(IgluConfigArg, str)
      node <- JsonUtils.extractJson(IgluConfigArg, raw)
    } yield node).toProcessingMessage

}
