/*
 * Copyright (c) 2012-2015 Snowplow Analytics Ltd. All rights reserved.
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

// Java
import java.util.UUID

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
import common.outputs.{
  EnrichedEvent,
  BadRow
}
import common.utils.shredder.Shredder

// Iglu Scala Client
import iglu.client.{
  ProcessingMessageNel,
  JsonSchemaPair,
  Resolver
}
import iglu.client.validation.ProcessingMessageMethods._

// This project
import inputs.EnrichedEventLoader
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
  def loadAndShred(line: String)(implicit resolver: Resolver): ValidatedNel[EventComponents] =
    for {
      event <- EnrichedEventLoader.toEnrichedEvent(line).toProcessingMessages
      fp     = getEventFingerprint(event)
      shred <- Shredder.shred(event)
    } yield (event.event_id, fp, shred)

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
  def projectBads(all: ValidatedNel[EventComponents]): Option[ProcessingMessageNel] =
    all.fold(
      e => Some(e), // Nel -> Some(List) of ProcessingMessages
      c => None)    // Discard

  /**
   * Projects our Successes into a
   * Some; everything else will be silently
   * dropped by Scalding in this pipeline. Note
   * that no contexts still counts as a Success
   * (as we want to copy atomic-events even if
   * no shredding was needed).
   *
   * @param all The Validation containing either
   *        our Successes or our Failures
   * @return an Option boxing either our List of
   *         Processing Messages on Failure, or
   *         None on Success
   */
  def projectGoods(all: ValidatedNel[EventComponents]): Option[EventComponents] = all.toOption

  // Have to define here so can be shared with tests
  import Dsl._
  val ShreddedPartition = new UrShreddedPartition('schema)

  // Indexes for the contexts, unstruct_event, and derived_contexts fields
  private val IgnoredJsonFields = Set(52, 58, 122)

  private val alteredEnrichedEventSubdirectory = "atomic-events"

  /**
   * Ready the enriched event for database load by removing JSON fields
   * and truncating field lengths based on Postgres' column types
   *
   * @param enrichedEvent TSV
   * @return the same TSV with the JSON fields removed
   */
  def alterEnrichedEvent(enrichedEvent: String): String = {
    import common.utils.ConversionUtils

    // TODO: move PostgresConstraints code out into Postgres-specific shredder when ready.
    // It's okay to apply Postgres constraints to events being loaded into Redshift as the PG
    // constraints are typically more permissive, but Redshift will be protected by the
    // COPY ... TRUNCATECOLUMNS.
    (enrichedEvent.split("\t", -1).toList.zipAll(PostgresConstraints.maxFieldLengths, "", None))
      .map { case (field, maxLength) =>
        maxLength match {
          case Some(ml) => ConversionUtils.truncate(field, ml)
          case None => field
        }
      }
      .zipWithIndex
      .filter(x => ! ShredJob.IgnoredJsonFields.contains(x._2))
      .map(_._1)
      .mkString("\t")
  }

  /**
   * Get the S3 path on which to store altered enriched events
   *
   * @param outFolder shredded/good/run=xxx
   * @return altered enriched event path
   */
  def getAlteredEnrichedOutputPath(outFolder: String): String = {
    val separator = if (outFolder.endsWith("/")) {
      ""
    } else {
      "/"
    }
    s"${outFolder}${separator}${alteredEnrichedEventSubdirectory}"
  }

  /**
   * Retrieves the event fingerprint. IF the field is null then we
   * assign a UUID at random for the fingerprint instead. This
   * is to respect any deduplication which requires both event ID and
   * event fingerprint to match.
   *
   * @param event The event to extract a fingerprint from
   * @return the event fingerprint
   */
  private def getEventFingerprint(event: EnrichedEvent): String =
    Option(event.event_fingerprint).getOrElse(UUID.randomUUID().toString)
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
  val goodJsonsOutput = PartitionedTsv(shredConfig.outFolder, ShredJob.ShreddedPartition, false, ('json), SinkMode.REPLACE)
  val badOutput = Tsv(shredConfig.badFolder)  // Technically JSONs but use Tsv for custom JSON creation
  implicit val resolver = shredConfig.igluResolver
  val goodEventsOutput = MultipleTextLineFiles(ShredJob.getAlteredEnrichedOutputPath(shredConfig.outFolder))

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
    .forceToDisk

  // Handle bad rows
  val bad = common
    .flatMap('output -> 'errors) { o: ValidatedNel[EventComponents] =>
      ShredJob.projectBads(o)
    }
    .mapTo(('line, 'errors) -> 'json) { both: (String, ProcessingMessageNel) =>
      new BadRow(both._1, both._2).toCompactJson
    }
    .write(badOutput)        // JSON containing line and error(s)

  // Handle good rows
  val good = common
    .flatMap('output -> ('eventId, 'eventFingerprint, 'good)) { o: ValidatedNel[EventComponents] =>
      ShredJob.projectGoods(o)
    }
    .groupBy('eventId, 'eventFingerprint) {
      _.take(1)
    }

  // Write atomic-events
  val events = good
    .mapTo('line -> 'altered) { s: String =>
      ShredJob.alterEnrichedEvent(s)
    }
    .write(goodEventsOutput)

  // Write JSONs
  val jsons = good
    .flatMapTo('good -> ('schema, 'json)) { pairs: List[JsonSchemaPair] =>
      pairs.map { pair =>
        (pair._1.toSchemaUri, pair._2.toString)
      }
    }
    .write(goodJsonsOutput)
}
