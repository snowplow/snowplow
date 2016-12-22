/*
 * Copyright (c) 2012-2017 Snowplow Analytics Ltd. All rights reserved.
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
import java.io.{PrintWriter, StringWriter}

// Scalaz
import scalaz.Validation

// Scala
import scala.util.control.NonFatal

// Cascading
import cascading.pipe.joiner.LeftJoin
import cascading.tap.SinkMode

// Scalding
import com.twitter.scalding._

// Snowplow Common Enrich
import common.{ FatalEtlError, UnexpectedEtlException }
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

// jackson
import com.fasterxml.jackson.databind.JsonNode

// AWS SDK
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughputExceededException

// json4s
import org.json4s.JObject
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods.{fromJsonNode, asJsonNode}

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
  def loadAndShred(line: String)(implicit resolver: Resolver): ValidatedNel[EventComponents] = {
    try {
      for {
        event <- EnrichedEventLoader.toEnrichedEvent(line).toProcessingMessages
        fp     = getEventFingerprint(event)
        shred <- Shredder.shred(event)
      } yield (event.event_id, fp, shred, event.etl_tstamp)
    } catch {
      case NonFatal(nf) =>
        val errorWriter = new StringWriter
        nf.printStackTrace(new PrintWriter(errorWriter))
        Validation.failure[String, EventComponents](s"Unexpected error processing events: $errorWriter").toProcessingMessageNel
    }
  }

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
      _ => None)    // Discard

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

  // Index for the event id
  private val EventIdIndex = 6
  private val CollectorTstampIndex = 3

  /**
   * Ready the enriched event for database load by removing JSON fields
   * and truncating field lengths based on Postgres' column types
   *
   * @param enrichedEvent TSV
   * @param newEventId A new event ID if required for a synthetic dupe
   * @return the same TSV with the JSON fields removed
   */
  def alterEnrichedEvent(enrichedEvent: String, newEventId: Option[String]): String = {
    import common.utils.ConversionUtils

    // TODO: move PostgresConstraints code out into Postgres-specific shredder when ready.
    // It's okay to apply Postgres constraints to events being loaded into Redshift as the PG
    // constraints are typically more permissive, but Redshift will be protected by the
    // COPY ... TRUNCATECOLUMNS.
    enrichedEvent.split("\t", -1).toList.zipAll(PostgresConstraints.maxFieldLengths, "", None)
      .map { case (field, maxLength) =>
        maxLength match {
          case Some(ml) => ConversionUtils.truncate(field, ml)
          case None => field
        }
      }
      .zipWithIndex
      .filter(x => ! ShredJob.IgnoredJsonFields.contains(x._2))
      .map { case (value, index) =>
        newEventId match {
          case Some(eid) if index == EventIdIndex => eid
          case _                                  => value
        }
      }
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
   * Creates a new event ID if the count of synthetic duplicates is greater
   * than 1.
   *
   * @param duplicateCount The count of synthetic duplicates
   * @return a possible new event ID, Option-boxed
   */
  private def generateNewEventId(duplicateCount: Long): Option[String] =
    Option(duplicateCount) match {
      case Some(dc) if dc > 1 => Some(UUID.randomUUID().toString)
      case _                  => None
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

  /**
   * Construct context for event containing duplicate event_id
   *
   * @param dupeEventId original UUID that have collision withing batch
   * @param newEventId new UUID generated by synthetic deduplication
   * @param enrichedLine enriched event TSV line to extract rootTstamp (collector timstamp)
   * @return some stringified duplicate context if `newEventId` was set
   */
  private def getDuplicationContextHierarchy(dupeEventId: String, newEventId: Option[String], enrichedLine: String): Option[(String, String)] =
    newEventId.map { rootId =>
      val collectorTstamp = enrichedLine.split("\t")(CollectorTstampIndex)
      (
        "iglu:com.snowplowanalytics.snowplow/duplicate/jsonschema/1-0-0",
        s"""|{
              |"schema":{"vendor":"com.snowplowanalytics.snowplow","name":"duplicate","format":"jsonschema","version":"1-0-0"},
              |"data":{"originalEventId":"$dupeEventId"},
              |"hierarchy":{"rootId":"$rootId","rootTstamp":"$collectorTstamp","refRoot":"events","refTree":["events","duplicate"],"refParent":"events"}
            |}""".stripMargin.replaceAll("[\n\r]","")
      )
    }

  /**
   * Transform shredded JSON hierarchy if original event had synthetic duplicate
   * Inject `newEventId` if it exists into `rootId`
   *
   * @param hierarchy shredded JSON hierarchy which possibly contains duped rootId
   * @param newEventId some new event id if original event was duped
   * @return original JSON if there event wasn't duped or updated one if it was
   */
  private def updateHierarchWithDeduplicatedId(hierarchy: JsonNode, newEventId: Option[String]): JsonNode = {
    val updatedHierarchy = newEventId.map { id: String =>
      val jsonObject = fromJsonNode(hierarchy).merge("hierarchy" -> ("rootId" -> id): JObject)
      asJsonNode(jsonObject)
    }
    
    updatedHierarchy.getOrElse(hierarchy)
  }
}

/**
 * The Snowplow Shred job, written in Scalding
 * (the Scala DSL on top of Cascading).
 */ 
class ShredJob(args: Args) extends Job(args) {

  // Job configuration. Scalaz recommends using fold()
  // for unpicking a Validation
  val shredConfig = ShredJobConfig.loadConfigFrom(args).fold(
    e => throw FatalEtlError(e.map(_.toString)),
    c => c)

  // Unpack duplicate storage. It can throw exception that can happen on initialization
  // Configuration parsing exception will be thrown on `shredConfig` unpack
  // Values are lazy to prevent serialization crash
  lazy val duplicateStorage: Option[DuplicateStorage] =
    shredConfig.duplicatesStorage.map(DuplicateStorage.initStorage) match {
      case Some(validation) => validation.fold(e => throw new FatalEtlError(e.toString), c => Some(c))
      case None => None       // Duplicate storage is optional
    }

  // Resolver is cloned because original one was initialized for one-time `DuplicateStorage` validation
  // and cannot be serialized anymore
  lazy val resolver = shredConfig.igluResolver.copy()

  // Aliases for our job
  val input = MultipleTextLineFiles(shredConfig.inFolder).read
  val goodJsonsOutput = PartitionedTsv(shredConfig.outFolder, ShredJob.ShreddedPartition, false, ('json), SinkMode.REPLACE)
  val badOutput = Tsv(shredConfig.badFolder)  // Technically JSONs but use Tsv for custom JSON creation
  val goodEventsOutput = MultipleTextLineFiles(ShredJob.getAlteredEnrichedOutputPath(shredConfig.outFolder))

  // Do we add a failure trap?
  val trappableInput = shredConfig.exceptionsFolder match {
    case Some(folder) => input.addTrap(Tsv(folder))
    case None => input
  }

  /**
    * Try to store event components to duplicate storage.
    * If event is unique in storage - true will be returned,
    * If event is already in storage, but with different etlTstamp - false will be returned,
    * If event is already in storage, but with same etlTstamp - true will be returned (previous shredding was interrupted),
    * If storage is not configured - true will be returned.
    * Function can be used as filter predicate
    *
    * @param components triple of event_id, event_fingerprint, etl_timestamp
    * @return true if event is unique, false otherwise
    */
  def dedupeCrossBatch(components: (String, String, String)): Boolean = {
    (components, duplicateStorage) match {
      case ((eventId, eventFingerprint, etlTstamp), Some(storage)) =>
        try {
          storage.put(eventId, eventFingerprint, etlTstamp)
        } catch {
          case e: ProvisionedThroughputExceededException =>
            throw UnexpectedEtlException(e.toString)
        }
      case _ => true
    }
  }

  // Scalding data pipeline
  val common = trappableInput
    .map('line -> 'output) { l: String =>
      ShredJob.loadAndShred(l)(resolver)
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
    .flatMap('output -> ('eventId, 'eventFingerprint, 'good, 'etlTstamp)) { o: ValidatedNel[EventComponents] =>
      ShredJob.projectGoods(o)
    }
    .groupBy('eventId, 'eventFingerprint) {
      _.take(1)               // Take only one event from group with identical eids and fingerprints
    }
    .filter(('eventId, 'eventFingerprint, 'etlTstamp)) { o: (String, String, String) =>
      dedupeCrossBatch(o)
    }

  // Now count synthetic dupes (same id, different fingerprint)
  // This should be pretty tiny
  val syntheticDupes = good
    .groupBy('eventId) { _.size('dupeCount) }
    .filter('dupeCount) { count: Long => count > 1 }
    .rename('eventId, 'dupeEventId)

  // Now join onto the dupes
  // Add new field with some newEventId for events that has dupes
  val goodWithSyntheticDupes = good   // (eventId?, dupeEventId?, good) ->
    .joinWithSmaller('eventId -> 'dupeEventId, syntheticDupes, joiner = new LeftJoin)
    .map('dupeCount -> 'newEventId) { count: Long =>
      ShredJob.generateNewEventId(count)
    }

  // Create `duplicate` JSON contexts for good events having dupes
  // This creates not canonical contexts (with schema and data), but shredded hierarchies
  val duplicateContexts = goodWithSyntheticDupes
    .flatMapTo(('dupeEventId, 'newEventId, 'line) -> ('schema, 'json)) { idsAndEvent: (String, Option[String], String) =>
      ShredJob.getDuplicationContextHierarchy(idsAndEvent._1, idsAndEvent._2, idsAndEvent._3)
    }

  // Replace old (duped) event_id with new one if it exists and write to output
  val events = goodWithSyntheticDupes
    .mapTo(('line, 'newEventId) -> 'altered) { both: (String, Option[String]) =>
      ShredJob.alterEnrichedEvent(both._1, both._2)
    }
    .write(goodEventsOutput)

  // Update shredded JSONs with new (deduped) event_ids, stringify, attach duplicate contexts
  // and write to output
  val jsons = (goodWithSyntheticDupes
    .flatMapTo(('good, 'newEventId) -> ('schema, 'json)) { jsonPairsWithId: (List[JsonSchemaPair], Option[String]) =>
      jsonPairsWithId._1.map { pair =>
        val hierarchy = ShredJob.updateHierarchWithDeduplicatedId(pair._2, jsonPairsWithId._2)
        (pair._1.toSchemaUri, hierarchy.toString)
      }
    } ++ duplicateContexts)
    .write(goodJsonsOutput)
}
