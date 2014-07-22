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

// Java
import java.net.URI

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
   * A helper to install files in the distributed
   * cache on HDFS.
   *
   * @param conf Our current job Configuration
   * @param filesToCache The files to install in
   *        the cache, a list of the URI to the
   *        file plus where it should be installed.
   */
  def installFilesInCache(conf: Configuration, filesToCache: List[(URI, String)]) {
    for (file <- filesToCache) {
      val hdfsPath = FileUtils.sourceFile(conf, file._1).valueOr(e => throw FatalEtlError(e.toString))
      FileUtils.addToDistCache(conf, hdfsPath, file._2)
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

  // This should really be in Scalding
  def isLocalMode(implicit mode: Mode) = !getJobConf.isDefined

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
 * The Snowplow ETL job, written in Scalding
 * (the Scala DSL on top of Cascading).
 *
 * Note that EtlJob has to be serializable by
 * Kyro - so be super careful what you add as
 * fields.
 */ 
class EtlJob(args: Args) extends Job(args) {

  // Job configuration. Scalaz recommends using fold()
  // for unpicking a Validation
  val (etlConfig, filesToCache) = EtlJobConfig.loadConfigAndFilesToCache(args, EtlJob.isLocalMode).fold(
    e => throw FatalEtlError(e.toString),
    c => (c._1, c._2))

  // Wait until we're on the nodes to instantiate with lazy
  // TODO: let's fix this Any typing
  lazy val loader = CollectorLoader.getLoader(etlConfig.inFormat).fold(
    e => throw FatalEtlError(e),
    c => c).asInstanceOf[CollectorLoader[Any]]

  // Wait until we're on the nodes to instantiate with lazy
  lazy val enrichmentRegistry = EtlJobConfig.reloadRegistryOnNode(etlConfig.enrichments, etlConfig.igluConfig, etlConfig.localMode)

  // Install MaxMind file(s) if we have them
  for (conf <- EtlJob.getJobConf) {
    EtlJob.installFilesInCache(conf, filesToCache)
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
