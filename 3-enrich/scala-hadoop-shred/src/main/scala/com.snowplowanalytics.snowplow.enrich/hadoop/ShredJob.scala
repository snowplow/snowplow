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
package com.snowplowanalytics
package snowplow
package enrich
package hadoop

// Cascading
import cascading.tap.SinkMode
import cascading.tuple.Fields

// Scala
import scala.collection.mutable.Buffer

// Scalaz
import scalaz._
import Scalaz._

// Scalding
import com.twitter.scalding._

// Snowplow Common Enrich
import common._
import common.FatalEtlError

// Iglu Scala Client
import iglu.client.{
  ProcessingMessageNel,
  JsonSchemaPair,
  Resolver
}
import iglu.client.validation.ProcessingMessageMethods._

// This project
import inputs.EnrichedEventLoader
import shredder.Shredder
import common.outputs.BadRow
import outputs.{
  ShreddedPartition => UrShreddedPartition
}

/**
 * Helpers for our data processing pipeline.
 */
object ShredJob {

  /**
   * Pipelines our loading of raw lines into
   * shredding the JSONs.
   *
   * @param line The incoming raw line (hopefully
   *        holding a Snowplow enriched event)
   * @param resolver Our implicit Iglu
   *        Resolver, for schema lookups
   * @return a Validation boxing either a Nel of
   *         ProcessingMessages on Failure, or a
   *         (possibly empty) List of JSON instances
   *         + schemas on Success
   */
  def loadAndShred(line: String)(implicit resolver: Resolver): ValidatedNel[JsonSchemaPairs] =
    for {
      event <- EnrichedEventLoader.toEnrichedEvent(line).toProcessingMessages
      shred <- Shredder.shred(event)
    } yield shred

  /**
   * Projects our Failures into a Some; Successes
   * become a None will be silently dropped by
   * Scalding in this pipeline.
   *
   * @param all The Validation containing either
   *        our Successes or our Failures
   * @return an Option boxing either our List of
   *         Processing Messages on Failure, or
   *         None on Success
   */
  def projectBads(all: ValidatedNel[JsonSchemaPairs]): Option[ProcessingMessageNel] =
    all.fold(
      e => Some(e), // Nel -> Some(List) of ProcessingMessages
      c => None)    // Discard

  /**
   * Projects our non-empty Successes into a
   * Some; everything else will be silently
   * dropped by Scalding in this pipeline.
   *
   * @param all The Validation containing either
   *        our Successes or our Failures
   * @return an Option boxing either our List of
   *         Processing Messages on Failure, or
   *         None on Success
   */
  def projectGoods(all: ValidatedNel[JsonSchemaPairs]): Option[List[JsonSchemaPair]] = all match {
    case Success(nel @ _ :: _) => Some(nel) // (Non-empty) List -> Some(List) of JsonSchemaPairs
    case _                     => None      // Discard
  }

  // Have to define here so can be shared with tests
  import Dsl._
  val ShreddedPartition = new UrShreddedPartition('schema)
}

/**
 * The Snowplow Shred job, written in Scalding
 * (the Scala DSL on top of Cascading).
 */ 
class ShredJob(args : Args) extends Job(args) {

  // Job configuration. Scalaz recommends using fold()
  // for unpicking a Validation
  val shredConfig = ShredJobConfig.loadConfigFrom(args).fold(
    e => throw FatalEtlError(e.map(_.toString)),
    c => c)

  // Aliases for our job
  val input = MultipleTextLineFiles(shredConfig.inFolder).read
  val goodOutput = PartitionedTsv(shredConfig.outFolder, ShredJob.ShreddedPartition, false, ('json), SinkMode.REPLACE)
  val badOutput = Tsv(shredConfig.badFolder)  // Technically JSONs but use Tsv for custom JSON creation
  implicit val resolver = shredConfig.igluResolver

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
    .flatMap('output -> 'errors) { o: ValidatedNel[JsonSchemaPairs] =>
      ShredJob.projectBads(o)
    }
    .mapTo(('line, 'errors) -> 'json) { both: (String, ProcessingMessageNel) =>
      BadRow(both._1, both._2).toCompactJson
    }
    .write(badOutput)        // JSON containing line and error(s)

  // Handle good rows
  val good = common
    .flatMapTo('output -> 'good) { o: ValidatedNel[JsonSchemaPairs] =>
      ShredJob.projectGoods(o)
    }
    .flatMapTo('good -> ('schema, 'json)) { pairs: List[JsonSchemaPair] =>
      pairs.map { pair =>
        (pair._1.toSchemaUri, pair._2.toString)
      }
    }
    .write(goodOutput)
}
