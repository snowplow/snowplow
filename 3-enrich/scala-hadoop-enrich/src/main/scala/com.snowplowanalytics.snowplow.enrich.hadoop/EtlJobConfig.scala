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
import java.util.NoSuchElementException

// Scalaz
import scalaz._
import Scalaz._

// Scalding
import com.twitter.scalding.Args

// Snowplow Common Enrich
import common.utils.ConversionUtils
import common.enrichments.PrivacyEnrichments.AnonOctets
import AnonOctets._

// This project
import utils.ScalazArgs

/**
 * The configuration for the SnowPlowEtlJob.
 */
case class EtlJobConfig(
    inFolder: String,
    inFormat: String,
    maxmindFile: URI,
    outFolder: String,
    badFolder: String,
    anonOctets: AnonOctets,
    exceptionsFolder: Option[String])

/**
 * Module to handle configuration for
 * the SnowPlowEtlJob
 */
object EtlJobConfig {

  /**
   * Convert the Maxmind file from a
   * String to a Validation[URI].
   *
   * @param maxmindFile A String holding the
   *        URI to the hosted MaxMind file
   * @return a Validation-boxed URI
   */
  private def getMaxmindUri(maxmindFile: String): Validation[String, URI] = {

    // TODO: fix compiler warning, match may not be exhaustive.
    // [warn] It would fail on the following input: None
    // [warn]     ConversionUtils.stringToUri(maxmindFile).flatMap(_ match {
    // [warn]                                                      ^
    // [warn] there were 1 feature warning(s); re-run with -feature for details
    ConversionUtils.stringToUri(maxmindFile).flatMap(_ match {
      case Some(u) => u.success
      case None => "URI to MaxMind file must be provided".fail
      })
  }

  /**
   * Convert a Stringly-typed integer
   * into the corresponding AnonQuartets
   * Enum Value.
   *
   * Update the Validation Error if the
   * conversion isn't possible.
   *
   * @param anonOctets A String holding
   *        the number of IP address
   *        quartets to anonymize
   * @return a Validation-boxed AnonQuartets
   */
  private def getAnonOctets(anonOctets: String): Validation[String, AnonOctets] = {
    try {
      AnonOctets.withName(anonOctets).success
    } catch {
      case nse: NoSuchElementException => "IP address quartets to anonymize must be 0, 1, 2, 3 or 4".fail
    }
  }

  /**
   * Loads the Config from the Scalding
   * job's supplied Args.
   *
   * @param args The arguments to parse
   * @return the EtLJobConfig, or one or
   *         more error messages, boxed
   *         in a Scalaz Validation Nel
   */
  def loadConfigFrom(args: Args): ValidationNel[String, EtlJobConfig] = {

    import ScalazArgs._
    val inFolder  = args.requiredz("input_folder")
    val inFormat = args.requiredz("input_format") // TODO: check it's a valid format
    val maxmindFile = args.requiredz("maxmind_file").flatMap(f => getMaxmindUri(f))
    val outFolder = args.requiredz("output_folder")
    val badFolder = args.requiredz("bad_rows_folder")
    val anonOctets = args.requiredz("anon_ip_quartets").flatMap(q => getAnonOctets(q))
    val exceptionsFolder = args.optionalz("exceptions_folder")
    
    (inFolder.toValidationNel |@| inFormat.toValidationNel |@| maxmindFile.toValidationNel |@| outFolder.toValidationNel |@| badFolder.toValidationNel |@| anonOctets.toValidationNel |@| exceptionsFolder.toValidationNel) { EtlJobConfig(_,_,_,_,_,_,_) }
  }
}