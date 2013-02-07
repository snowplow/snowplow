/*
 * Copyright (c) 2012-2013 SnowPlow Analytics Ltd. All rights reserved.
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

// Cascading

// Scalding
import com.twitter.scalding._

// This project
import utils.Json2Line // TODO: remove when we can
import enrichments.EnrichmentManager

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

  // Aliases for our job
  val loader = etlConfig.collectorLoader
  val input = MultipleTextLineFiles(etlConfig.inFolder)
  val goodOutput = Tsv(etlConfig.outFolder)
  val badOutput = Json2Line(etlConfig.errFolder)

  // Scalding data pipeline
  val common = input
    .read
    .map('line -> 'output) { l: String =>
      EtlJobFlow.toCanonicalOutput(loader.toCanonicalInput(l))
    }

  // Handle bad rows
  val bad = common
    .flatMap('output -> 'errors) { o: ValidatedMaybeCanonicalOutput => o.fold(
      e => Some(e.toList), // NEL -> Some(List)
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
    .mapTo('good -> 'unboxed) { g: MaybeCanonicalOutput => g.get.toString() }
    .write(goodOutput)
}