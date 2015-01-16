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
import com.twitter.scalding.commons.source._

// Cascading
import cascading.tuple.Fields
import cascading.tap.SinkMode
import cascading.pipe.Pipe

// Snowplow Common Enrich
import common._
import common.utils.ConversionUtils
import common.FatalEtlError
import common.loaders.Loader
import common.enrichments.{
  EnrichmentRegistry,
  EnrichmentManager
}
import enrichments.registry._
import common.outputs.{
  EnrichedEvent,
  BadRow
}

// This project
import utils.FileUtils
import generated.ProjectSettings

/**
 * Holds constructs to help build the ETL job's data
 * flow (see below).
 */ 
object EtlJob {

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
  val etlVersion = s"hadoop-${ProjectSettings.version}"

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
   * @param all A List of Validations each containing either
   *        an EnrichedEvent on Success or Failure Strings
   * @return a (possibly empty) List of failures, where
   *         each failure is represented by a NEL of
   *         Strings
   */
  def projectBads(all: List[ValidatedEnrichedEvent]): List[NonEmptyList[String]] = all
    .map(_.swap.toOption)
    .collect { case Some(errs) =>
      errs
    }

  /**
   * Projects our non-empty Successes into a
   * Some; everything else will be silently
   * dropped by Scalding in this pipeline.
   *
   * @param all A List of Validations each containing either
   *        an EnrichedEvent on Success or Failure Strings
   * @return a (possibly empty) List of EnrichedEvents
   */
  def projectGoods(all: List[ValidatedEnrichedEvent]): List[EnrichedEvent] = all
    .map(_.toOption)
    .collect { case Some(gd) =>
      gd
    }
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
  lazy val loader = Loader.getLoader(etlConfig.inFormat).fold(
    e => throw FatalEtlError(e),
    c => c).asInstanceOf[Loader[Any]]

  // Wait until we're on the nodes to instantiate with lazy
  implicit lazy val igluResolver = EtlJobConfig.reloadResolverOnNode(etlConfig.igluConfig)
  lazy val enrichmentRegistry = EtlJobConfig.reloadRegistryOnNode(etlConfig.enrichments, etlConfig.localMode)

  // Install MaxMind file(s) if we have them
  for (conf <- EtlJob.getJobConf) {
    EtlJob.installFilesInCache(conf, filesToCache)
  }

  // Aliases for our job
  val input = getInputPipe(etlConfig.inFormat, etlConfig.inFolder)
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
    .map('line -> 'all) { l: Any =>
      EtlPipeline.processEvents(enrichmentRegistry, EtlJob.etlVersion, etlConfig.etlTstamp, loader.toCollectorPayload(l))
    }

  // Handle bad rows
  val bad = common
    .map('all -> 'errors) { o: List[ValidatedEnrichedEvent] =>
      EtlJob.projectBads(o)
    } // : List[NonEmptyList[String]]
    .flatMapTo(('line, 'errors) -> 'json) { both: (String, List[NonEmptyList[String]]) =>
      for {
        error <- both._2
        bad    = BadRow(both._1, error).toCompactJson
      } yield bad // : List[BadRow]
    }   
    .write(badOutput) // N JSONs containing line and error(s)

  // Handle good rows
  val good = common
    .flatMapTo('all -> 'events) { o: List[ValidatedEnrichedEvent] =>
      EtlJob.projectGoods(o)
    } // : List[EnrichedEvent]
    .unpackTo[EnrichedEvent]('events -> '*)
    .write(goodOutput) // N EnrichedEvents as tuples

  /**
   * Determine the Scalding Source to use based on the inFormat configuration
   *
   * @param format the inFormat configuration
   * @param path
   * @return FixedPathLzoRaw for LZO-compressed thrift, otherwise MultipleTextFiles
   */
  def getInputPipe(format: String, path: String): Pipe = {
    import TDsl._
    format match {
      case "thrift" => FixedPathLzoRaw(path).toPipe('line)
      case _ => MultipleTextLineFiles(path).read
    }
  }
}
