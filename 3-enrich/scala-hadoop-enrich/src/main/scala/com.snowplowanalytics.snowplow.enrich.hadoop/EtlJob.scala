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

// Hadoop
import org.apache.hadoop.conf.Configuration

// Scalaz
import scalaz._
import Scalaz._

// Scalding
import com.twitter.scalding._

// Snowplow Common Enrich
import common._
import common.utils.ConversionUtils
import common.FatalEtlError
import common.inputs.CollectorLoader
import common.enrichments.{
  EnrichmentRegistry,
  EnrichmentManager
}
import enrichments.registry._
import common.outputs.CanonicalOutput

// This project
import utils.FileUtils
import generated.ProjectSettings
import outputs.BadRow

/**
 * Holds constructs to help build the ETL job's data
 * flow (see below).
 */ 
object EtlJob {

  /**
   * A helper method to take a ValidatedMaybeCanonicalInput
   * and flatMap it into a ValidatedMaybeCanonicalOutput.
   *
   * We have to do some unboxing because enrichEvent
   * expects a raw CanonicalInput as its argument, not
   * a MaybeCanonicalInput.
   *
   * @param registry Contains configuration for all
   *        enrichments to apply   
   * @param etlTstamp The ETL timestamp
   * @param input The ValidatedMaybeCanonicalInput   
   * @return the ValidatedMaybeCanonicalOutput. Thanks to
   *         flatMap, will include any validation errors
   *         contained within the ValidatedMaybeCanonicalInput
   */
  def toCanonicalOutput(registry: EnrichmentRegistry, etlTstamp: String, input: ValidatedMaybeCanonicalInput): ValidatedMaybeCanonicalOutput = {
    input.flatMap {
      _.cata(EnrichmentManager.enrichEvent(registry, etlVersion, etlTstamp, _).map(_.some),
             none.success)
    }
  }

  /**
   * A helper to source the MaxMind data file(s) we 
   * use for IP location -> geo-location lookups
   *
   * How we source each MaxMind data file depends
   * on whether we are running locally or on HDFS:
   *
   * 1. On HDFS - source the file at `hdfsPath`,
   *    add it to Hadoop's distributed cache and
   *    return the symlink
   * 2. On local (test) - find the copy of the
   *    file on our resource path (downloaded for
   *    us by SBT) and return that path
   *
   * @param conf Our current job Configuration
   * @param ipLookupsEnrichment The configured IpLookupsEnrichment
   */
  def installIpLookupsFiles(conf: Configuration, ipLookupsEnrichment: IpLookupsEnrichment) {
    for (kv <- ipLookupsEnrichment.cachePathMap; cachePath <- kv._2) { // We have a distributed cache to install to
      val hdfsPath = FileUtils.sourceFile(conf, ipLookupsEnrichment.lookupMap(kv._1)._1).valueOr(e => throw FatalEtlError(e.toString))
      FileUtils.addToDistCache(conf, hdfsPath, cachePath)
    }
  }

  /** 
   * Generate our "host ETL" version string.
   * @return our version String
   */
  val etlVersion = "hadoop-%s" format ProjectSettings.version

  // This should really be in Scalding
  def getJobConf(implicit mode: Mode): Option[Configuration] = {
    mode match {
      case Hdfs(_, conf) => Option(conf)
      case _ => None
    }
  }

  /**
   * Projects our Failures into a Some; Successes
   * become a None will be silently dropped by
   * Scalding in this pipeline.
   *
   * @param in The Validation containing either
   *        our Successes or our Failures
   * @return an Option boxing either our List of
   *         Strings on Failure, or None on
   *         Success
   */
  def projectBads(in: ValidatedMaybeCanonicalOutput): Option[NonEmptyList[String]] =
    in.fold(
      e => Some(e), // Nel -> Some(List)
      c => None)    // Discard
}

/**
 * The SnowPlow ETL job, written in Scalding
 * (the Scala DSL on top of Cascading).
 */ 
class EtlJob(args: Args) extends Job(args) {

  // Very first thing we do is grab the Hadoop conf
  val confOption: Option[Configuration] = EtlJob.getJobConf

  // Job configuration. Scalaz recommends using fold()
  // for unpicking a Validation
  val (etlConfig, registry) = EtlJobConfig_.loadConfigAndRegistry(args, !confOption.isDefined).fold(
    e => throw FatalEtlError(e.toString),
    c => (c._1, c._2))

  // Wait until we're on the nodes to instantiate with lazy
  // TODO: let's fix this Any typing
  lazy val loader = CollectorLoader.getLoader(etlConfig.inFormat).fold(
    e => throw FatalEtlError(e),
    c => c).asInstanceOf[CollectorLoader[Any]]

  // Wait until we're on the nodes to instantiate with lazy
  lazy val enrichmentRegistry = EtlJobConfig_.reloadRegistryOnNode(etlConfig.enrichments, etlConfig.igluConfig, etlConfig.localMode)

  // Only install MaxMind file(s) if enrichment is enabled
  for (ipLookupsEnrichment <- registry.getIpLookupsEnrichment) {
    for (conf <- confOption) {
      EtlJob.installIpLookupsFiles(conf, ipLookupsEnrichment)
    }
  }

  // Aliases for our job
  val input = MultipleTextLineFiles(etlConfig.inFolder).read
  val goodOutput = Tsv(etlConfig.outFolder)
  val badOutput = Tsv(etlConfig.badFolder)

  // Do we add a failure trap?
  val trappableInput = etlConfig.exceptionsFolder match {
    case Some(folder) => input.addTrap(Tsv(folder))
    case None => input
  }

  // Scalding data pipeline
  // TODO: let's fix this Any typing
  val common = trappableInput
    .map('line -> 'output) { l: Any =>
      EtlJob.toCanonicalOutput(enrichmentRegistry, etlConfig.etlTstamp, loader.toCanonicalInput(l))
    }

  // Handle bad rows
  val bad = common
    .flatMap('output -> 'errors) { o: ValidatedMaybeCanonicalOutput =>
      EtlJob.projectBads(o)
    }
    .mapTo(('line, 'errors) -> 'json) { both: (String, NonEmptyList[String]) =>
      BadRow(both._1, both._2).toCompactJson
    }    
    .write(badOutput) // JSON containing line and error(s)

  // Handle good rows
  val good = common
    .flatMapTo('output -> 'good) { o: ValidatedMaybeCanonicalOutput =>
      o match {
        case Success(Some(s)) => Some(s)
        case _ => None // Drop errors *and* blank rows
      }
    }
    .unpackTo[CanonicalOutput]('good -> '*)
    .write(goodOutput)
}
