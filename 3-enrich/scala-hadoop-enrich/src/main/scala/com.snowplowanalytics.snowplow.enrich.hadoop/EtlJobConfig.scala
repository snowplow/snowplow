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
package com.snowplowanalytics
package snowplow
package enrich
package hadoop

// Java
import java.net.URI
import java.util.NoSuchElementException

// Jackson
import com.fasterxml.jackson.databind.JsonNode

// Scalaz
import scalaz._
import Scalaz._

// json4s
import org.json4s.jackson.JsonMethods._

// Scalding
import com.twitter.scalding.Args

// Iglu
import iglu.client._
import iglu.client.validation.ProcessingMessageMethods._

// Snowplow Common Enrich
import common._
import common.utils.{
  ConversionUtils,
  JacksonJsonUtils
}

import common.enrichments.{
  EnrichmentConfigRegistry,
  EventEnrichments
}

// This project
import utils.ScalazArgs

/**
 * The configuration for the SnowPlowEtlJob.
 *
 * @param inFolder The processing folder
 * @param inFormat The format in which the
 *        collector is saving data 
 * @param outFolder Folder where good rows
 *        are stored
 * @param badFolder Folder where bad rows
 *        are stored
 * @param etlTstamp Time at which ETL occurred
 * @param registry Iglu registry for schema
 *        validation
 * @param exceptionsFolder Folder where
 *        exceptions are stored
 */
case class EtlJobConfig(
  inFolder: String,
  inFormat: String,
  outFolder: String,
  badFolder: String,
  etlTstamp: String,
  registry: EnrichmentConfigRegistry,
  exceptionsFolder: Option[String]
  )

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
   * @param localMode Whether to use the
   *        local MaxMind data file. 
   *        Enabled for tests.
   * @return the EtLJobConfig, or one or
   *         more error messages, boxed
   *         in a Scalaz Validation Nel
   */
  def loadConfigFrom(args: Args, localMode: Boolean): ValidatedNelMessage[EtlJobConfig] = {

    import ScalazArgs._

    val inFolder  = args.requiredz("input_folder")
    val inFormat = args.requiredz("input_format") // TODO: check it's a valid format
    val outFolder = args.requiredz("output_folder")
    val badFolder = args.requiredz("bad_rows_folder")
    val etlTstamp = args.requiredz("etl_tstamp").flatMap(t => EventEnrichments.extractTimestamp("etl_tstamp", t).leftMap(_.toProcessingMessage))
    val exceptionsFolder = args.optionalz("exceptions_folder")
    
    val igluResolver: ValidatedNelMessage[Resolver] = args.requiredz("iglu_config") match {
      case Failure(e) => e.toString.toProcessingMessage.failNel[Resolver]
      case Success(s) => for {
        node <- base64ToJsonNode(s)
        reso <- Resolver.parse(node)
      } yield reso
    }

    val enrichments: ValidatedNelMessage[JsonNode] = for {
      str  <- (args.requiredz("enrichments").toValidationNel: ValidatedNelMessage[String])
      node <-  base64ToJsonNode(str)
      } yield node

    val registry: ValidatedNelMessage[EnrichmentConfigRegistry] = (enrichments |@| igluResolver) {
      buildEnrichmentRegistry(_, localMode)(_)
    }.flatMap(s => s)

    (inFolder.toValidationNel |@| inFormat.toValidationNel |@| outFolder.toValidationNel |@| badFolder.toValidationNel |@| etlTstamp.toValidationNel |@| registry |@| exceptionsFolder.toValidationNel) { EtlJobConfig(_,_,_,_,_,_,_) }
  }

  /**
   * Builds an EnrichmentRegistry from the enrichments arg
   *
   * @param enrichments The JSON of all enrichments
   *        constructed by EmrEtlRunner
   * @param localMode Whether to use the local
   *        MaxMind data file. Enabled for tests.
   * @param resolver (implicit) The Iglu resolver used
   *        for schema lookup and validation
   */
  private def buildEnrichmentRegistry(enrichments:JsonNode, localMode: Boolean)(implicit resolver: Resolver): ValidatedNelMessage[EnrichmentConfigRegistry] = {
    EnrichmentConfigRegistry.parse(fromJsonNode(enrichments), localMode)
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
  private def base64ToJsonNode(str: String): ValidatedNelMessage[JsonNode] =
    (for {
      raw <-  ConversionUtils.decodeBase64Url("enrichments", str)
      node <- JacksonJsonUtils.extractJson("enrichments", raw)
    } yield node).leftMap(_.toProcessingMessage).toValidationNel
}
