/*
 * Copyright (c) 2012 SnowPlow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.hadoop.etl

// Scalaz
import scalaz._
import Scalaz._

// Scalding
import com.twitter.scalding._

// This project
// TODO

/**
 * The SnowPlow ETL job, written in Scalding
 * (the Scala DSL on top of Cascading).
 */ 
class EtlJob(args: Args) extends Job(args) {

  // Job configuration. Scalaz recommends using fold()
  // for unpicking a Validation
  val etlConfig = EtlJobConfig.loadConfigFrom(args).fold(
    e => throw FatalEtlException(e),
    c => c)
  val Loader = etlConfig.collectorLoader // Alias

  val input = MultipleTextLineFiles(etlConfig.inFolder)
  val goodOutput = TextLine(etlConfig.outFolder)
  val badOutput = JsonLine("ohnoes/")

  // Scalding data pipeline
  input
    .read
    .mapTo('line -> 'input) { l: String =>
      val ci = Loader.toCanonicalInput(line)
      flatify(ci)
    }
    .project('input)
    .filter('input) { i : MaybeCanonicalInput =>
      i != Success(None) }
    .write(goodOutput)

  // Prototyping here
  def flatify(input: MaybeCanonicalInput) = input match {
    case Success(Some(s)) => "SUCCESS"
    case Success(None)    => "NONE"
    case Failure(f)       => {
      throw FatalEtlException(f)
      "OH NOES"
    }
  }
}