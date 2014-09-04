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
import iglu.client.Resolver
import iglu.client.validation.ProcessingMessageMethods._

// Snowplow Common Enrich
import common._
import common.utils.{
  ConversionUtils,
  JsonUtils
}

import common.enrichments.{
  EnrichmentRegistry,
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
  igluConfig: String,
  enrichments: String,
  localMode: Boolean,
  exceptionsFolder: Option[String]
  )

/**
 * Module to handle loading of EtlJobConfig, the
 * case class holding  configuration for the
 * EtlJob. Underscore at the end to keep these
 * functions from being added to the EtlJobConfig
 * as static class methods (which would break
 * serialization).
 */
object EtlJobConfig {

  /**
   * Loads the EtlJobConfig and the
   * EnrichmentRegistry from the Scalding
   * job's supplied Args. Returns the two
   * instances separately because the
   * EnrichmentRegistry cannot be serialized.
   *
   * @param args The arguments to parse
   * @param localMode Whether to use the
   *        local MaxMind data file. 
   *        Enabled for tests.
   * @return a Tuple2 containg the EtLJobConfig
   *         and the EnrichmentRegistry, or one
   *         or more error messages, boxed in a
   *         Scalaz Validation Nel.
   */
  def loadConfigAndFilesToCache(args: Args, localMode: Boolean): ValidatedNelMessage[Tuple2[EtlJobConfig, List[(URI, String)]]] = {

    import ScalazArgs._

    val inFolder  = args.requiredz("input_folder")
    val inFormat = args.requiredz("input_format") // TODO: check it's a valid format
    val outFolder = args.requiredz("output_folder")
    val badFolder = args.requiredz("bad_rows_folder")
    val etlTstamp = args.requiredz("etl_tstamp").flatMap(t => EventEnrichments.extractTimestamp("etl_tstamp", t).leftMap(_.toProcessingMessage))
    val exceptionsFolder = args.optionalz("exceptions_folder")
    
    val igluConfig = args.requiredz("iglu_config")
    val igluResolver: ValidatedNelMessage[Resolver] = igluConfig match {
      case Failure(e) => e.toString.toProcessingMessage.failNel[Resolver]
      case Success(s) => for {
        node <- base64ToJsonNode(s)
        reso <- Resolver.parse(node)
      } yield reso
    }

    val enrichments = args.requiredz("enrichments")
    val enrichmentsNode: ValidatedNelMessage[JsonNode] = for {
      str  <- (enrichments.toValidationNel: ValidatedNelMessage[String])
      node <-  base64ToJsonNode(str)
      } yield node

    val registry: ValidatedNelMessage[EnrichmentRegistry] = (enrichmentsNode |@| igluResolver) {
      buildEnrichmentRegistry(_, localMode)(_)
    }.flatMap(s => s)

    val filesToCache = registry match {
      case Success(reg) =>
        reg.getIpLookupsEnrichment match {
          case Some(ipLookups) => ipLookups.dbsToCache
          case None => Nil
        }
      case Failure(f) => Nil
    }

    // Discard registry (and rebuild later) because it causes serialization problems
    (inFolder.toValidationNel          |@|
      inFormat.toValidationNel         |@|
      outFolder.toValidationNel        |@|
      badFolder.toValidationNel        |@|
      etlTstamp.toValidationNel        |@|
      igluConfig.toValidationNel       |@|
      enrichments.toValidationNel      |@|
      exceptionsFolder.toValidationNel |@|
      registry) { (ifo, ifm, ofo, bfo, tms, ic, en, efo, _) =>
        (EtlJobConfig(ifo, ifm, ofo, bfo, tms, ic, en, localMode, efo), filesToCache)
      }
  }

  /**
   * Reload the Iglu Resolver once we are on the nodes.
   * This avoids Kyro serialization of the resolver,
   * which might fail (haven't tested if it does).
   *
   * @param igluConfig The JSON specifying Iglu repos
   * @return the instantiated Iglu Resolver
   */
  def reloadResolverOnNode(igluConfig: String): Resolver =
    (for {
        node <- base64ToJsonNode(igluConfig)
        reso <- Resolver.parse(node)
      } yield reso)
      .valueOr(e => throw new FatalEtlError(e.toString))

  /**
   * Reload the registry once we are on the nodes.
   * This avoids Kyro serialization of the registry,
   * which fails.
   *
   * @param enrichments The JSON array of enrichment
   *        configurations
   * @param localMode Whether we are running in local
   *        mode (i.e. not on a Hadoop cluster)
   * @param resolver (implicit) The Iglu resolver used
   *        for schema lookup and validation
   * @return the instantiated EnrichmentRegistry
   */
  def reloadRegistryOnNode(enrichments: String, localMode: Boolean)(implicit resolver: Resolver): EnrichmentRegistry =
    (for {
        node <- base64ToJsonNode(enrichments)
        reg  <- buildEnrichmentRegistry(node, localMode)
      } yield reg)
      .valueOr(e => throw new FatalEtlError(e.toString))

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
  private def buildEnrichmentRegistry(enrichments: JsonNode, localMode: Boolean)(implicit resolver: Resolver): ValidatedNelMessage[EnrichmentRegistry] =
    EnrichmentRegistry.parse(fromJsonNode(enrichments), localMode)

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
      raw  <- ConversionUtils.decodeBase64Url("enrichments", str)
      node <- JsonUtils.extractJson("enrichments", raw)
    } yield node).leftMap(_.toProcessingMessage).toValidationNel
}
