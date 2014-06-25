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

// Scala MaxMind GeoIP
import com.snowplowanalytics.maxmind.geoip.IpGeo

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
   * A helper to source the MaxMind data file we 
   * use for IP location -> geo-location lookups
   *
   * How we source the MaxMind data file depends
   * on whether we are running locally or on HDFS:
   *
   * 1. On HDFS - source the file at `hdfsPath`,
   *    add it to Hadoop's distributed cache and
   *    return the symlink
   * 2. On local (test) - find the copy of the
   *    file on our resource path (downloaded for
   *    us by SBT) and return that path
   *
   * @param fileUri The URI to the Maxmind GeoLiteCity.dat file
   * @param conf Our current job Configuration
   * @param ipToGeoEnrichment The configured IpToGeoEnrichment
   */
  def installIpGeoFile(conf: Configuration, ipToGeoEnrichment: IpToGeoEnrichment) {
    for (cachePath <- ipToGeoEnrichment.cachePath) { // We have a distributed cache to install to
      val hdfsPath = FileUtils.sourceFile(conf, ipToGeoEnrichment.uri).valueOr(e => throw FatalEtlError(e.toString))
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
  val etlConfig = EtlJobConfig.loadConfigFrom(args, !confOption.isDefined).fold(
    e => throw FatalEtlError(e.toString),
    c => c)

  // Wait until we're on the nodes to instantiate with lazy
  // TODO: let's fix this Any typing
  lazy val loader = CollectorLoader.getLoader(etlConfig.inFormat).fold(
    e => throw FatalEtlError(e),
    c => c).asInstanceOf[CollectorLoader[Any]]

  val enrichmentRegistry = etlConfig.registry

  // Only install file if enrichment is enabled
  for (ipToGeo <- enrichmentRegistry.getIpToGeoEnrichment) {
    for (conf <- confOption) {
      EtlJob.installIpGeoFile(conf, ipToGeo)
    }
  }

  // Aliases for our job
  val input = MultipleTextLineFiles(etlConfig.inFolder).read
  val goodOutput = Tsv(etlConfig.outFolder)
  val badOutput = JsonLine(etlConfig.badFolder)

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
    .flatMap('output -> 'errors) { o: ValidatedMaybeCanonicalOutput => o.fold(
      e => Some(e.toList), // Nel -> Some(List)
      c => None)
    }
    .project('line, 'errors)
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
