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
import inputs.CanonicalInput

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
  val goodOutput = TextLine(etlConfig.outFolder)
  val badOutput = JsonLine(etlConfig.errFolder)

  // Scalding data pipeline
  val common = input
    .read
    .map('line -> 'input) { l: String =>
      loader.toCanonicalInput(l)
    }

  // Handle bad rows
  val bad = common
    .map('input -> 'errors) { i: MaybeCanonicalInput =>
      i match { // Have to unbox for Scalding
        case Failure(f) => f.toList // NEL -> List
        case _ => Nil
      }
    }
    .filter('errors) { e: List[String] => !e.isEmpty } // Drop anything that isn't an error
    .project('line, 'errors)
    .write(badOutput) // JSON containing line and error(s)

  // Handle good rows
  val good = common
    .mapTo('input -> 'good) { i : MaybeCanonicalInput =>
      i match { // Have to unbox for Scalding
        case Success(Some(s)) => s
        case _ => null // Yech, kills the Success(None)s and Failure()s
      }
    }
    .filter('good) { g: CanonicalInput => g != null } // Yech, drop the nulls
    .write(goodOutput)
}