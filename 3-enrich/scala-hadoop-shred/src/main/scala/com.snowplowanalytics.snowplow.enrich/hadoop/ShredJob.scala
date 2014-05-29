/*
 * Copyright (c) 2012 Twitter, Inc.
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

// Jackson
// import com.github.fge.jsonschema.core.report.ProcessingMessage

// Scalaz
import scalaz._
import Scalaz._

// Scalding
import com.twitter.scalding._

// Snowplow Common Enrich
import common._
import common.FatalEtlError
import common.outputs.CanonicalOutput

// This project
import inputs.EnrichedEventLoader
import shredder.Shredder
import outputs.{
  BadRow,
  ShreddedPartition
}
import utils.ProcessingMessageUtils

/**
 * Helpers for our data processing pipeline.
 */
object ShredJob {

  import utils.ProcessingMessageUtils._

  /**
   * Pipelines our loading of raw lines into
   * shredding the JSONs.
   *
   * @param line The incoming raw line (hopefully
   *        holding a Snowplow enriched event)
   * @return a Validation boxing either a Nel of
   *         ProcessingMessages on Failure, or a
   *         (possibly empty) List of JSON instances
   *         + schemas on Success
   */
  def loadAndShred(line: String): ValidatedJsonSchemaPairList =
    for {
      event <- EnrichedEventLoader.toEnrichedEvent(line).toProcessingMessages
      shred <- Shredder.shred(event)
    } yield shred

  /**
   * Isolates our Failures into a Some; Successes
   * become a None will be silently dropped by
   * Scalding in this pipeline.
   *
   * @param all The Validation containing either
   *        our Successes or our Failures
   * @return an Option boxing either our List of
   *         Processing Messages on Failure, or
   *         None on Success
   */
  def isolateBads(all: ValidatedJsonSchemaPairList): MaybeProcMsgNel =
    all.fold(
      e => Some(e), // Nel -> Some(List) of ProcessingMessages
      c => None)
}

/**
 * The Snowplow Shred job, written in Scalding
 * (the Scala DSL on top of Cascading).
 */ 
class ShredJob(args : Args) extends Job(args) {

  // Job configuration. Scalaz recommends using fold()
  // for unpicking a Validation
  val shredConfig = ShredJobConfig.loadConfigFrom(args).fold(
    e => throw FatalEtlError(e),
    c => c)

  // Aliases for our job
  val input = MultipleTextLineFiles(shredConfig.inFolder).read
  val goodOutput = Tsv(shredConfig.outFolder) // Technically JSONs but use Tsv for partitioning
  val badOutput = Tsv(shredConfig.badFolder)  // Technically JSONs but use Tsv for custom JSON creation

  // Do we add a failure trap?
  val trappableInput = shredConfig.exceptionsFolder match {
    case Some(folder) => input.addTrap(Tsv(folder))
    case None => input
  }

  // Scalding data pipeline
  val common = trappableInput
    .map('line -> 'output) { l: String =>
      ShredJob.loadAndShred(l)
    }

  // Handle bad rows
  val bad = common
    .flatMap('output -> 'errors) { o: ValidatedJsonSchemaPairList =>
      ShredJob.isolateBads(o)
    }
    .mapTo(('line, 'errors) -> 'json) { both: (String, ProcMsgNel) =>
      BadRow(both._1, both._2).asJsonString
    }
    .write(badOutput)        // JSON containing line and error(s)

  // Handle good rows
  // TODO: implement this
  val good = input
    .flatMap('line -> 'word) { line : String => tokenize(line) }
    .groupBy('word) { _.size }
    .write( goodOutput )

  // Split a piece of text into individual words.
  def tokenize(text : String) : Array[String] = {
    // Lowercase each word and remove punctuation.
    text.toLowerCase.replaceAll("[^a-zA-Z0-9\\s]", "").split("\\s+")
  }
}
