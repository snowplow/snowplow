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

// Scalaz
import scalaz._
import Scalaz._

// Scala
import scala.util.control.NonFatal

// Jackson
import com.fasterxml.jackson.databind.JsonNode

// JSON Schema Validator
import com.github.fge.jsonschema.core.report.ProcessingMessage

// Iglu client
import com.snowplowanalytics.iglu.client.{Resolver, ValidatedNel}
import com.snowplowanalytics.iglu.client.validation.ProcessingMessageMethods.toProcMsgNel

// Joda time
import org.joda.time.DateTime

// This project
import DuplicateStorage._

object Main {

  /**
    * Raw CLI arguments
    */
  private[eventpopulator] case class RawJobConf(
    since: Option[String],
    enrichedInBucket: String,
    b64StorageConfig: String,
    b64ResolverConfig: String)

  private[this] val rawJobConf = RawJobConf(None, "", "", "")

  /**
    * Parsed job configuration with normalized values
    *
    * @param since date since when backpopulation should begin
    * @param enrichedInBucket enriched archive
    * @param storageConfig parsed and validated DynamoDB
    */
  case class JobConf(
    since: Option[DateTime],
    enrichedInBucket: String,
    storageConfig: DuplicateStorageConfig)

  private val parser = new scopt.OptionParser[RawJobConf](generated.ProjectMetadata.name) {
    head(generated.ProjectMetadata.name, generated.ProjectMetadata.version)

    opt[String]('i', "enriched-archive").required()
      .action((x, c) => c.copy(enrichedInBucket = normalizeBucket(x)))
      .text("S3 Bucket with enriched events archive")
      .validate(s => if(s.startsWith("s3://")) success else failure("--enrichedIn S3 path should start with s3://"))

    opt[String]('c', "storage-config").required().action((x, c) =>
      c.copy(b64StorageConfig = x)).text("Base64-encoded AWS DynamoDB storage configuration JSON")

    opt[String]('s', "since").action((x, c) =>
      c.copy(since = Some(x))).text("Populate storage starting from this date. Possible formats: YYYY-MM-dd-HH-mm-ss OR YYYY-MM-dd")

    opt[String]('r', "resolver").required().action((x, c) =>
      c.copy(b64ResolverConfig = x)).text("Base64-encoded Iglu resolver configuration JSON")
  }

  def main(args: Array[String]): Unit = {
    parse(args) match {
      case Some(Success(config)) => BackpopulateJob.run(config)
      case Some(Failure(failureNel)) =>
        val message = s"Unable to parse arguments: \n${failureNel.list.mkString("\n")}"
        sys.error(message)
      case None => sys.exit(1)
    }
  }

  /**
    * Parse command line arguments into job configuration object
    */
  def parse(args: Array[String]): Option[ValidatedNel[JobConf]] =
    parser.parse(args, rawJobConf).map(transform)

  /**
    * Transform raw CLI options into normalized configuration double checking its values
    */
  def transform(jobConf: RawJobConf): ValidatedNel[JobConf] = jobConf match {
    case RawJobConf(since, enrichedInBucket, b64StorageConfig, b64ResolverConfig) =>
      val storage = for {
        resolverConfig <- Utils.base64ToJsonNode(b64ResolverConfig).toValidationNel[ProcessingMessage, JsonNode]
        resolver <- Resolver.parse(resolverConfig)
        storageConfig <- DynamoDbConfig.extractFromBase64(b64StorageConfig, resolver)
      } yield storageConfig

      (storage |@| since.map(parseSinceTimestamp).sequenceU) { (storageConfig, since) =>
        JobConf(since, enrichedInBucket, storageConfig)
      }
  }

  /**
    * Parse `--since` value
    */
  private def parseSinceTimestamp(str: String): ValidatedNel[DateTime] =
    try {
      Utils.parseInput(str).successNel
    } catch {
      case NonFatal(e) =>
        val error = s"Error parsing --since option. It should conform YYYY-MM-dd-HH-mm-ss OR YYYY-MM-dd pattern.\n${e.toString}"
        toProcMsgNel(error).failure
    }

  /**
    * Drop s3 protocol and make sure there's trailing slash
    */
  private def normalizeBucket(bucket: String): String = {
    val schemaless = bucket.drop(5) // drop s3:// prefix
    if (schemaless.endsWith("/")) schemaless else schemaless + "/"
  }
}
