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

// Scala MaxMind GeoIP
import com.snowplowanalytics.maxmind.geoip.IpGeo

// Snowplow Common Enrich
import common._
import common.FatalEtlError
import common.inputs.CollectorLoader
import common.enrichments.EnrichmentManager
import common.outputs.CanonicalOutput
import common.enrichments.PrivacyEnrichments.AnonOctets.AnonOctets

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
   * @param input The ValidatedMaybeCanonicalInput
   * @return the ValidatedMaybeCanonicalOutput. Thanks to
   *         flatMap, will include any validation errors
   *         contained within the ValidatedMaybeCanonicalInput
   */
  def toCanonicalOutput(geo: IpGeo, anonOctets: AnonOctets, input: ValidatedMaybeCanonicalInput): ValidatedMaybeCanonicalOutput = {
    input.flatMap {
      _.cata(EnrichmentManager.enrichEvent(geo, etlVersion, anonOctets, _).map(_.some),
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
   * @return the path to the Maxmind GeoLiteCity.dat to use
   */
  def installIpGeoFile(fileUri: URI): String = {
    jobConfOption match {
      case Some(conf) => {   // We're on HDFS
        val hdfsPath = FileUtils.sourceFile(conf, fileUri).valueOr(e => throw FatalEtlError(e))
        FileUtils.addToDistCache(conf, hdfsPath, "geoip")
      }
      case None =>           // We're in local mode
        getClass.getResource("/maxmind/GeoLiteCity.dat").toURI.getPath
    }
  }

  /**
   * A helper to create the new IpGeo object.
   *
   * @param ipGeoFile The path to the MaxMind GeoLiteCity.dat file
   * @return an IpGeo object ready to perform IP->geo lookups
   */
  def createIpGeo(ipGeoFile: String): IpGeo =
    IpGeo(ipGeoFile, memCache = true, lruCache = 20000)

  /** 
   * Generate our "host ETL" version string.
   * @return our version String
   */
  val etlVersion = "hadoop-%s" format ProjectSettings.version

  // This should really be in Scalding
  private def jobConfOption(implicit mode: Mode): Option[Configuration] = {
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

  // Job configuration. Scalaz recommends using fold()
  // for unpicking a Validation
  val etlConfig = EtlJobConfig.loadConfigFrom(args).fold(
    e => throw FatalEtlError(e),
    c => c)

  // Wait until we're on the nodes to instantiate with lazy
  // TODO: let's fix this Any typing
  lazy val loader = CollectorLoader.getLoader(etlConfig.inFormat).fold(
    e => throw FatalEtlError(e),
    c => c).asInstanceOf[CollectorLoader[Any]]

  val ipGeoFile = EtlJob.installIpGeoFile(etlConfig.maxmindFile)
  lazy val ipGeo = EtlJob.createIpGeo(ipGeoFile)

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
      EtlJob.toCanonicalOutput(ipGeo, etlConfig.anonOctets, loader.toCanonicalInput(l))
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