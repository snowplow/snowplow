/*
 * Copyright (c) 2017 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.eventpopulator

// scalaz
import scalaz.{Ordering => _, _}
import Scalaz._

// Scala
import scala.util.control.NonFatal
import scala.collection.convert.decorateAsScala._

// AWS SDK
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.s3.model.ListObjectsRequest

// Java
import java.nio.charset.StandardCharsets.UTF_8

// Jackson
import com.fasterxml.jackson.databind.{ ObjectMapper, JsonNode }

// Iglu client
import com.snowplowanalytics.iglu.client.Validated
import com.snowplowanalytics.iglu.client.validation.ProcessingMessageMethods._

// Apache Commons Codec
import org.apache.commons.codec.binary.Base64

// Joda time
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.joda.time.format.DateTimeFormatterBuilder

// This project
import Main.JobConf

/**
  * Util functions and constant that not directly related to spark job
  */
object Utils {

  // Column indexes in TSV
  val EtlTimestampIndex = 2
  val EventIdIndex = 6
  val FingerprintIndex = 129

  /**
    * Date format in archive buckets: run=YYYY-MM-dd-HH-mm-ss
    * In EmrEtlRunner: "%Y-%m-%d-%H-%M-%S"
    */
  val runIdFormat = DateTimeFormat.forPattern("YYYY-MM-dd-HH-mm-ss")

  /**
    * Shortened `runIdFormat` that can be used for manual input
    */
  val dayFormat = DateTimeFormat.forPattern("YYYY-MM-dd")

  /**
    * Combined `dayFormat` and `runIdFormat` that can be used
    * both for precise and shorten manual input
    */
  val inputFormatter = new DateTimeFormatterBuilder()
    .append(null, List(runIdFormat.getParser, dayFormat.getParser).toArray)
    .toFormatter

  implicit def dateTimeOrdering: Ordering[DateTime] =
    Ordering.fromLessThan(_ isBefore _)

  private val UrlSafeBase64 = new Base64(true) // true means "url safe"

  private lazy val Mapper = new ObjectMapper

  /**
    * Data extracted from EnrichedEvent and storing in DynamoDB
    */
  case class DeduplicationTriple(eventId: String, fingerprint: String, etlTstamp: String)

  /**
    * Error happening when TSV doesn't match expected number of lines
    *
    * @param columns encountered number of columns
    * @param line original event TSV line
    */
  case class ParsingError(columns: Int, line: String) extends RuntimeException {
    def message: String = s"Wrong EnrichedEvent TSV columns number. Expected 130, got [$columns]. Line: [$line]"
  }

  /**
    * Get run ids from `jobConf.enrichedInBucket` happened since `jobConf.since`
    *
    * @param jobConf input configuration
    * @return list of timestamps (run ids)
    */
  def getRuns(jobConf: JobConf): List[String] = {
    val runIds = ls3s(jobConf)
    val filtered = jobConf.since match {
      case Some(start) => filterSince(runIds, start)
      case None => runIds
    }
    filtered.map(runId => runIdFormat.print(runId))
  }

  /**
    * Get list of all folders inside S3 bucket with enriched archive
    *
    * @param jobConf parameters parsed from command line
    * @return list of **dates** (without "run") of for arhives
    */
  private def ls3s(jobConf: JobConf): List[DateTime] = {
    val credentials = new DefaultAWSCredentialsProviderChain()
    val s3Client = AmazonS3ClientBuilder.standard().withCredentials(credentials).build()
    val (bucket, subpath) = splitS3Path(jobConf.enrichedInBucket)
    val req = new ListObjectsRequest(bucket, subpath, null, "/", null)
    val objects = s3Client.listObjects(req).getCommonPrefixes
    objects.asScala.toList.flatMap(_.split("=").lift(1)).flatMap(parseRunId)
  }

  /**
    * Parse run id from enriched event archive dir
    *
    * @param date run timestamp (part after `run=`)
    * @return datetime object if parsed successfully
    */
  def parseRunId(date: String): Option[DateTime] = {
    val runId = if (date.endsWith("/")) date.dropRight(1) else date
    try {
      Some(DateTime.parse(runId, runIdFormat))
    } catch {
      case _: IllegalArgumentException => None
    }
  }

  /**
    * Parse date from user input. Precise or shorten
    *
    * @param date timestamp string that can be date or datetime
    * @return datetime object
    */
  def parseInput(date: String): DateTime =
    DateTime.parse(date, inputFormatter)

  /**
    * Get sorted list of run ids (dates) started after some point in time
    */
  def filterSince(runs: List[DateTime], start: DateTime): List[DateTime] =
    runs.sorted.dropWhile(run => run.isBefore(start))

  /**
    * Split S3 path into bucket name and prefix
    *
    * @param path S3 full path without `s3://` prefix and with trailing slash
    * @return pair of bucket name and remaining path
    */
  private def splitS3Path(path: String): (String, String) = { // TODO: check that it works on root level bucket
    path.split("/").toList match {
      case head :: Nil => (head, "/")
      case head :: tail => (head, tail.mkString("/") + "/")
    }
  }

  /**
    * Split `EnrichedEvent` TSV line and extract necessary columns
    *
    * @param line plain `EnrichedEvent` TSV
    * @return deduplication triple encapsulated into special class
    */
  def lineToTriple(line: String): DeduplicationTriple = {
    val tsv = line.split('\t')
    try {
      DeduplicationTriple(eventId = tsv(EventIdIndex), etlTstamp = tsv(EtlTimestampIndex), fingerprint = tsv(FingerprintIndex))
    } catch {
      case e: IndexOutOfBoundsException => throw new RuntimeException(s"ERROR: Cannot split TSV [$line]\n${e.toString}")
    }
  }

  /**
    * Converts a base64-encoded JSON string into a JsonNode
    *
    * @param str base64-encoded JSON
    * @return a JsonNode on Success, a NonEmptyList of ProcessingMessages on Failure
    */
  def base64ToJsonNode(str: String): Validated[JsonNode] =
    (for {
      raw  <- decodeBase64Url(str)
      node <- extractJson(raw)
    } yield node).toProcessingMessage

  def extractJson(instance: String): Validation[String, JsonNode] =
    try {
      Mapper.readTree(instance).success
    } catch {
      case NonFatal(e) => s"Invalid JSON [%s] with parsing error: %s".format(instance, e.getMessage).failure
    }

  def decodeBase64Url(str: String): Validation[String, String] = {
    try {
      val decodedBytes = UrlSafeBase64.decode(str)
      new String(decodedBytes, UTF_8).success
    } catch {
      case NonFatal(e) =>
        "Exception while decoding Base64-decoding string [%s] (URL-safe encoding): [%s]".format(str, e.getMessage).failure
    }
  }
}
