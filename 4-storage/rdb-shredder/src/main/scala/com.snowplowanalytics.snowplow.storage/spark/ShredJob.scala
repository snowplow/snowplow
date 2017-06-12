/*
 * Copyright (c) 2012-2017 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at
 * http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and
 * limitations there under.
 */
package com.snowplowanalytics
package snowplow
package storage.spark

import java.io.{PrintWriter, StringWriter}
import java.util.UUID

import scala.util.control.NonFatal

// Jackson
import com.fasterxml.jackson.databind.JsonNode

// Spark
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession

// Scalaz
import scalaz._
import Scalaz._

// AWS SDK
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughputExceededException

// Snowplow
import iglu.client.{JsonSchemaPair, ProcessingMessageNel, Resolver, SchemaKey}
import iglu.client.validation.ProcessingMessageMethods._
import enrich.common.{FatalEtlError, UnexpectedEtlException, ValidatedNelMessage}
import enrich.common.utils.shredder.Shredder
import enrich.common.outputs.{BadRow, EnrichedEvent}

/** Helpers method for the shred job */
object ShredJob extends SparkJob {
  private[spark] val classesToRegister: Array[Class[_]] = Array(
    classOf[Array[String]],
    classOf[Shredded],
    classOf[SchemaKey],
    classOf[java.util.LinkedHashMap[_, _]],
    classOf[java.util.ArrayList[_]],
    classOf[scala.collection.immutable.Map$EmptyMap$],
    classOf[scala.collection.immutable.Set$EmptySet$],
    classOf[com.fasterxml.jackson.databind.node.ObjectNode],
    classOf[com.fasterxml.jackson.databind.node.TextNode],
    classOf[com.fasterxml.jackson.databind.node.BooleanNode],
    classOf[com.fasterxml.jackson.databind.node.DoubleNode],
    classOf[com.fasterxml.jackson.databind.node.FloatNode],
    classOf[com.fasterxml.jackson.databind.node.IntNode],
    classOf[com.fasterxml.jackson.databind.node.LongNode],
    classOf[com.fasterxml.jackson.databind.node.ShortNode],
    classOf[com.fasterxml.jackson.databind.node.ArrayNode],
    classOf[com.fasterxml.jackson.databind.node.NullNode],
    classOf[com.fasterxml.jackson.databind.node.JsonNodeFactory],
    classOf[org.apache.spark.internal.io.FileCommitProtocol$TaskCommitMessage]
  )
  override def sparkConfig(): SparkConf = new SparkConf()
    .setAppName(getClass().getSimpleName())
    .setIfMissing("spark.master", "local[*]")
    .set("spark.yarn.maxAppAttempts", "1")
    .set("spark.serializer", classOf[KryoSerializer].getName())
    .registerKryoClasses(classesToRegister)

  override def run(spark: SparkSession, args: Array[String]): Unit = {
    val job = ShredJob(spark, args)
    job.run()
  }

  def apply(spark: SparkSession, args: Array[String]) = new ShredJob(spark, args)

  /**
   * Pipeline the loading of raw lines into shredded JSONs.
   * @param line The incoming raw line (hopefully holding a Snowplow enriched event)
   * @param resolver The implicit Iglu resolver used for schema lookups
   * @return a Validation boxing either a Nel of ProcessingMessages on Failure,
   *         or a (possibly empty) List of JSON instances + schema on Success
   */
  def loadAndShred(
    line: String
  )(implicit resolver: Resolver): ValidatedNelMessage[EventComponents] =
    try {
      for {
        event <- inputs.EnrichedEventLoader.toEnrichedEvent(line).toProcessingMessages
        fp     = getEventFingerprint(event)
        shred <- Shredder.shred(event)
      } yield (event.event_id, fp, shred, event.etl_tstamp)
    } catch {
      case NonFatal(nf) =>
        val errorWriter = new StringWriter
        nf.printStackTrace(new PrintWriter(errorWriter))
        Validation.failure[String, EventComponents](
          s"Unexpected error processing events: $errorWriter").toProcessingMessageNel
    }

  /**
   * Retrieve the event fingerprint. If the field is null then we assign a random UUID.
   * This is to respect any deduplication which requires both event ID and event fingerprint to
   * match.
   * @param event The event to extract a fingerprint from
   * @return the event fingerprint or a random UUID
   */
  def getEventFingerprint(event: EnrichedEvent): String =
    Option(event.event_fingerprint).getOrElse(UUID.randomUUID().toString)

  /**
   * Project Failures into a Some; Successes become a None and will be dropped.
   * @param line The incoming raw line which will be kept if there are Failures
   * @param all The validation containing either Sucesses or Failures
   * @return an Option boxing either our List of ProcessingMessages on Failure, or None on Success
   */
  def projectBads(
    line: String,
    all: ValidatedNelMessage[EventComponents]
  ): Option[(String, ProcessingMessageNel)] =
    all.fold(
      e => Some((line, e)),
      _ => None // Discard
    )

  /**
   * Project Successes into a Some; Failures become a None and will be dropped.
   * Note that no contexts still count as a Success (as we want to copy atomic-events even if no
   * shredding was needed).
   * @param all The validation containing either Successes or Failures
   * @return an Option boxing either the event components on Success or None on Failure
   */
  def projectGoods(all: ValidatedNelMessage[EventComponents]): Option[EventComponents] =
    all.toOption

  /**
   * Create a new event ID if the count of synthetic duplicates is greater than 1.
   * @param duplicateCount The count of synthetic duplicates
   * @return an Option containing a random UUID if there more than 1 synthetic duplicates, None
   *         otherwise
   */
  def generateNewEventId(duplicateCount: Option[Long]): Option[String] =
    duplicateCount match {
      case Some(dc) if dc > 1 => Some(UUID.randomUUID().toString())
      case None               => None
    }

  /**
   * Construct context for event with duplicate event ID.
   * @param dupeEventId Original UUID for which there is a collision within this batch
   * @param newEventId New UUID generated by synthetic deduplication
   * @param enrichedLine Enriched event TSV line from which we extract the collector timestamp
   * @return A stringified duplicate context if newEventId contained a Some
   */
  def getDuplicationContextHierarchy(
    dupeEventId: String,
    newEventId: Option[String],
    enrichedLine: String
  ): Option[(String, String)] =
    newEventId.map { rootId =>
      val collectorTstampIndex = 3
      val collectorTstamp = enrichedLine.split("\t")(collectorTstampIndex)
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
   * Ready the enriched event for database load by removing a few JSON fields and truncating field
   * lengths based on Postgres' column types.
   * @param originalLine The original TSV line
   * @param newEventId A new event ID present in case of a synthetic duplicate
   * @return The original line with the proper fields removed respecting the Postgres constaints
   */
  def alterEnrichedEvent(originalLine: String, newEventId: Option[String]): String = {
    import com.snowplowanalytics.snowplow.enrich.common.utils.ConversionUtils

    // TODO: move PostgresConstraints code out into Postgres-specific shredder when ready.
    // It's okay to apply Postgres constraints to events being loaded into Redshift as the PG
    // constraints are typically more permissive, but Redshift will be protected by the
    // COPY ... TRUNCATECOLUMNS.
    val ignoredJsonFieldIndices = Set(52, 58, 122)
    val eventIdIndex = 6
    originalLine.split("\t", -1).toList.zipAll(utils.PostgresConstraints.maxFieldLengths, "", None)
      .map { case (field, maxLength) =>
        maxLength match {
          case Some(ml) => ConversionUtils.truncate(field, ml)
          case None => field
        }
      }
      .zipWithIndex
      .filter(x => !ignoredJsonFieldIndices.contains(x._2))
      .map { case (value, index) =>
        newEventId match {
          case Some(eid) if index == eventIdIndex => eid
          case _                                  => value
        }
      }
      .mkString("\t")
  }

  /**
   * Transform the shredded JSON hierarchy if the original event had synthetic duplicates.
   * Inject a new event id if present as root id.
   * @param hierarchy Shredded JSON hierarchy which may contain duplicated rood id
   * @param newEventId Some event id if the original event was duplicated
   * @return the original JSON if the event hadn't been duplicated, a new one with an updated root
   *         id otherwise
   */
  def updateHierarchyWithDedupedId(hierarchy: JsonNode, newEventId: Option[String]): JsonNode = {
    import org.json4s.jackson.JsonMethods.{fromJsonNode, asJsonNode}
    import org.json4s.JObject
    import org.json4s.JsonDSL._
    newEventId
      .map { id =>
        val jsonObject = fromJsonNode(hierarchy).merge("hierarchy" -> ("rootId" -> id): JObject)
        asJsonNode(jsonObject)
      }
      .getOrElse(hierarchy)
  }

  /**
   * The path at which to store the altered enriched events.
   * @param outFolder shredded/good/run=xxx
   * @return The altered enriched event path
   */
  def getAlteredEnrichedOutputPath(outFolder: String): String = {
    val alteredEnrichedEventSubdirectory = "atomic-events"
    s"${outFolder}${if (outFolder.endsWith("/")) "" else "/"}${alteredEnrichedEventSubdirectory}"
  }

  /**
   * The path at which to store the shredded types.
   * @param outFolder shredded/good/run=xxx
   * @return The shredded types output path
   */
  def getShreddedTypesOutputPath(outFolder: String): String = {
    val shreddedTypesSubdirectory = "shredded-types"
    s"${outFolder}${if (outFolder.endsWith("/")) "" else "/"}${shreddedTypesSubdirectory}"
  }

  /**
   * Try to store event components in duplicate storage and check if it was stored before
   * If event is unique in storage - true will be returned,
   * If event is already in storage, with different etlTstamp - false will be returned,
   * If event is already in storage, but with same etlTstamp - true will be returned (previous shredding was interrupted),
   * If storage is not configured - true will be returned.
   * If provisioned throughput exception happened - interrupt whole job
   * If other runtime exception happened - failure is returned to be used as bad row
   * @param components triple of event_id, event_fingerprint, etl_timestamp
   * @param duplicateStorage object dealing with possible duplicates
   * @return boolean inside validation, denoting presence or absence of event in storage
   */
  @throws[UnexpectedEtlException]
  def dedupeCrossBatch(
    components: (String, String, String), duplicateStorage: Option[DuplicateStorage]
  ): ValidatedNelMessage[Boolean] =
    (components, duplicateStorage) match {
      case ((eventId, eventFingerprint, etlTstamp), Some(storage)) =>
        try {
          Success(storage.put(eventId, eventFingerprint, etlTstamp))
        } catch {
          case e: ProvisionedThroughputExceededException =>
            throw UnexpectedEtlException(e.toString)
          case NonFatal(e) =>
            Failure(s"Cross-batch deduplication unexpected failure: $e}").toProcessingMessageNel
        }
      case _ => Success(true)
    }
}

/**
 * Case class representing shredded events.
 * @param eventId ID of the event
 * @param eventFingerprint Fingerprint of the event
 * @param shreds List of (SchemaKey, JsonNode) pairs
 * @param etlTstamp Etl timestamp
 * @param originalLine The line as it was before shredding (Snowplow enriched event)
 */
case class Shredded(
  eventId: String,
  eventFingerprint: String,
  shreds: List[JsonSchemaPair],
  etlTstamp: String,
  originalLine: String
)

/**
 * Case class represeting good events and synthetic duplicates.
 * @param eventId ID of the event
 * @param newEventId New ID generated for synthetic duplicates
 * @param shredded Shredded event built earlier in the pipeline
 */
case class Event(eventId: String, newEventId: Option[String], shredded: Shredded)

/**
 * The Snowplow Shred job, written in Spark.
 * @param spark Spark session used throughout the job
 * @param args Command line arguments for the shred job
 */
class ShredJob(@transient val spark: SparkSession, args: Array[String]) extends Serializable {
  @transient private val sc: SparkContext = spark.sparkContext
  import spark.implicits._
  import singleton._

  // Job configuration
  private val shredConfig = ShredJobConfig.loadConfigFrom(args)
    .valueOr(e => throw new FatalEtlError(e.toString))

  private val dupStorageConfig = DuplicateStorage.DynamoDbConfig.extract(
    shredConfig.duplicateStorageConfig.success,
    ResolverSingleton.getIgluResolver(shredConfig.igluConfig)
  ).valueOr(e => throw new FatalEtlError(e.toString))

  // We try to build DuplicateStorage early to detect failures before starting the job and create
  // the table if it doesn't exist
  @transient private val _ = DuplicateStorageSingleton.get(dupStorageConfig)

  /**
   * Runs the shred job by:
   *  - shredding the Snowplow enriched events
   *  - separating out malformed rows from the properly-formed
   *  - finding synthetic duplicates and adding them back with new ids
   *  - writing out JSON contexts as well as properly-formed and malformed events
   */
  def run(): Unit = {
    import ShredJob._

    val input = sc.textFile(shredConfig.inFolder)

    val common = input
      .map(line => (line, loadAndShred(line)(ResolverSingleton.get(shredConfig.igluConfig))))
      .cache()

    // Handling of malformed rows
    val bad = common
      .flatMap { case (line, shredded) => projectBads(line, shredded) }
      .map { case (line, errors) => new BadRow(line, errors).toCompactJson }

    // Handling of properly-formed rows, only one event from an event id and event fingerprint
    // combination is kept
    val good = common
      .flatMap { case (line, shredded) => projectGoods(shredded).map((_, line)) }
      .map { case (shred, line) => Shredded(shred._1, shred._2, shred._3, shred._4, line) }
      .groupBy(s => (s.eventId, s.eventFingerprint))
      .flatMap { case (_, vs) => vs.take(1) }
      .map { s =>
        val absent = dedupeCrossBatch((s.eventId, s.eventFingerprint, s.etlTstamp),
          DuplicateStorageSingleton.get(dupStorageConfig))
        (s, absent)
      }
      .cache()

    // Deduplication operation succeeded
    val dupeSucceeded = good
      .filter {
        case (_, Success(r)) => r
        case (_, Failure(_)) => false
      }
      .cache()

    // Count synthetic duplicates, defined as events with the same id but different fingerprints
    val syntheticDupes = dupeSucceeded
      .map(_._1)
      .groupBy(_.eventId)
      .filter { case (_, vs) => vs.size > 1 }
      .map { case (k, vs) => (k, vs.size.toLong) }

    // Join the properly-formed events with the synthetic duplicates, generate a new event ID for
    // those that are synthetic duplicates
    val goodWithSyntheticDupes = dupeSucceeded
      .map(_._1)
      .map(s => s.eventId -> s)
      .leftOuterJoin(syntheticDupes)
      .map { case (eventId, (shredded, cnt)) =>
        Event(eventId, generateNewEventId(cnt), shredded)
      }
      .cache()

    // Deduplication operation failed due to DynamoDB
    val dupeFailed = good.filter(_._2.isFailure)

    // Write errors unioned with errors occurred during cross-batch deduplication
    val badDupes = bad ++ dupeFailed
      .flatMap { case (s, dupe) => dupe match {
        case Failure(m) => Some(new BadRow(s.originalLine, m).toCompactJson)
        case _ => None
      }}
    badDupes.saveAsTextFile(shredConfig.badFolder)

    // Create duplicate JSON contexts for well-formed events having duplicates
    // This creates not canonical contexts (with schema and data), but shredded hierarchies
    val duplicateContexts = goodWithSyntheticDupes
      .flatMap { e =>
        getDuplicationContextHierarchy(e.eventId, e.newEventId, e.shredded.originalLine)
      }

    // Ready the events for database load
    val events = goodWithSyntheticDupes
      .map(e => alterEnrichedEvent(e.shredded.originalLine, e.newEventId))
    events.saveAsTextFile(getAlteredEnrichedOutputPath(shredConfig.outFolder))

    // Update the shredded JSONs with the new deduplicated event IDs and stringify
    val jsons = (goodWithSyntheticDupes
      .flatMap { e =>
        e.shredded.shreds.map { pair =>
          val hierarchy = updateHierarchyWithDedupedId(pair._2, e.newEventId)
          (pair._1.toSchemaUri, hierarchy.toString())
        }
      } ++ duplicateContexts)
      .flatMap { case (s, j) =>
        SchemaKey.parse(s) match {
          case Success(k) => Some((k.vendor, k.name, k.format, k.version, j))
          case _          => None
        }
      }
    jsons
      .toDF("vendor", "name", "format", "version", "json")
      .write
      .partitionBy("vendor", "name", "format", "version")
      .mode("append")
      .text(getShreddedTypesOutputPath(shredConfig.outFolder))
  }
}
