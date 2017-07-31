 /*
 * Copyright (c) 2013-2016 Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache
 * License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at
 * http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied.
 *
 * See the Apache License Version 2.0 for the specific language
 * governing permissions and limitations there under.
 */
package com.snowplowanalytics.snowplow.enrich
package kinesis

// Java
import java.io.File
import java.net.URI

// Amazon
import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.services.dynamodbv2.model.ScanRequest
import com.amazonaws.services.dynamodbv2.model.AttributeValue
import com.amazonaws.services.dynamodbv2.document.DynamoDB
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model.GetObjectRequest

// Logging
import org.slf4j.LoggerFactory

// Scala
import sys.process._
import scala.collection.JavaConverters._
import scala.annotation.tailrec

// Config
import com.typesafe.config.{
  Config,
  ConfigFactory
}

// Scalaz
import scalaz.{Sink => _, _}
import Scalaz._

// json4s
import org.json4s.jackson.JsonMethods._
import org.json4s.JsonDSL._

// Iglu
import com.snowplowanalytics.iglu.client.Resolver

// Snowplow
import common.enrichments.EnrichmentRegistry
import common.utils.JsonUtils
import sources._
import sinks._

/**
 * The main entry point for Stream Enrich.
 */
object KinesisEnrichApp {

  lazy val log = LoggerFactory.getLogger(getClass())
  import log.{error, debug, info, trace}

  val FilepathRegex = "^file:(.+)$".r
  val DynamoDBRegex = "^dynamodb:([^/]*)/([^/]*)/([^/]*)$".r
  val regexMsg = "'file:[filename]' or 'dynamodb:[region/table/key]'"

  case class EnrichConfig(
    config: File = new File("."),
    resolver: String = "",
    enrichmentsDir: Option[String] = None
  )

  def main(args: Array[String]): Unit = {
    val parser = new scopt.OptionParser[EnrichConfig](generated.Settings.name) {
      head(generated.Settings.name, generated.Settings.version)
      opt[File]("config").required().valueName("<filename>")
        .action((f: File, c: EnrichConfig) => c.copy(config = f))
        .validate(f =>
          if (f.exists) success
          else failure(s"Configuration file $f does not exist")
        )
      opt[String]("resolver").required().valueName("<resolver uri>")
        .text(s"Iglu resolver file, $regexMsg")
        .action((r: String, c: EnrichConfig) => c.copy(resolver = r))
        .validate(_ match {
          case FilepathRegex(_) | DynamoDBRegex(_) => success
          case _ => failure("Resolver doesn't match accepted uris: $regexMsg")
        })
      opt[String]("enrichments").optional().valueName("<enrichment directory uri>")
        .text(s"Directory of enrichment configuration JSONs, $regexMsg")
        .action((e: String, c: EnrichConfig) => c.copy(enrichmentsDir = Some(e)))
        .validate(_ match {
          case FilepathRegex(_) | DynamoDBRegex(_) => success
          case _ => failure("Enrichments directory doesn't match accepted uris: $regexMsg")
        })
    }

    parser.parse(args, EnrichConfig()) match {
      case Some(config) => 
      case None => System.exit(-1)
    }
  }

  def main(ec: EnrichConfig): Unit = {
    val parsedConfig = ConfigFactory.parseFile(ec.config).resolve()

    val kinesisEnrichConfig = new KinesisEnrichConfig(parsedConfig)
    val provider = kinesisEnrichConfig.credentialsProvider

    val tracker = if (parsedConfig.hasPath("enrich.monitoring.snowplow")) {
      SnowplowTracking.initializeTracker(
        parsedConfig.getConfig("enrich.monitoring.snowplow")).some
    } else { 
      None 
    }

    implicit val igluResolver: Resolver = (for {
      parsedResolver <- extractResolver(provider, ec.resolver)
      json <- JsonUtils.extractJson("", parsedResolver)
      resolver <- Resolver.parse(json).leftMap(_.toString)
    } yield resolver) fold (
      e => throw new RuntimeException(e),
      s => s
    )

    val registry: EnrichmentRegistry = (for {
      enrichmentConfig <- extractEnrichmentConfig(provider, ec.enrichmentsDir)
      registryConfig <- JsonUtils.extractJson("", enrichmentConfig)
      reg <- EnrichmentRegistry.parse(fromJsonNode(registryConfig), false).leftMap(_.toString)
    } yield reg) fold (
      e => throw new RuntimeException(e),
      s => s
    )

    val filesToCache = registry.getIpLookupsEnrichment match {
      case Some(ipLookups) => ipLookups.dbsToCache
      case None => Nil
    }

    for (uriFilePair <- filesToCache) {
      val targetFile = new File(uriFilePair._2)

      // Ensure uri does not have doubled slashes
      val cleanUri = new java.net.URI(uriFilePair._1.toString.replaceAll("(?<!(http:|https:|s3:))//", "/"))

      // Download the database file if it doesn't already exist or is empty
      // See http://stackoverflow.com/questions/10281370/see-if-file-is-empty
      if (targetFile.length == 0L) {

        // Check URI Protocol and download file
        val downloadResult: Int = cleanUri.getScheme match {
          case "http" | "https" => (cleanUri.toURL #> targetFile).! // using sys.process
          case "s3"             => downloadFromS3(provider, cleanUri, targetFile)
          case s => throw new RuntimeException(s"Schema ${s} for file ${cleanUri} not supported")
        }

        if (downloadResult != 0) {
          throw new RuntimeException(s"Attempt to download ${cleanUri} to $targetFile failed")
        }
      }
    }

    val source = kinesisEnrichConfig.source match {
      case Source.Kafka => new KafkaSource(kinesisEnrichConfig, igluResolver, registry, tracker)
      case Source.Kinesis => new KinesisSource(kinesisEnrichConfig, igluResolver, registry, tracker)
      case Source.Stdin => new StdinSource(kinesisEnrichConfig, igluResolver, registry, tracker)
    }

    tracker match {
      case Some(t) => SnowplowTracking.initializeSnowplowTracking(t)
      case None    => None
    }

    source.run
  }

  /**
   * Return a JSON string based on the resolver argument
   * @param provider AWS credentials provider
   * @param resolverArgument location of the resolver
   * @return JSON from a local file or stored in DynamoDB
   */
  def extractResolver(
    provider: AWSCredentialsProvider,
    resolverArgument: String
  ): Validation[String, String] = resolverArgument match {
    case FilepathRegex(filepath) => {
      val file = new File(filepath)
      if (file.exists) {
        scala.io.Source.fromFile(file).mkString.success
      } else {
        "Iglu resolver configuration file \"%s\" does not exist".format(filepath).failure
      }
    }
    case DynamoDBRegex(region, table, key) =>
      lookupDynamoDBConfig(provider, region, table, key).success
    case _ => s"Resolver argument [$resolverArgument] must begin with 'file:' or 'dynamodb:'".failure
  }

  /**
   * Fetch configuration from DynamoDB
   * Assumes the primary key is "id" and the configuration's key is "json"
   * @param prodiver AWS credentials provider
   * @param region DynamoDB region, e.g. "eu-west-1"
   * @param table
   * @param key The value of the primary key for the configuration
   * @return The JSON stored in DynamoDB
   */
  def lookupDynamoDBConfig(provider: AWSCredentialsProvider,
      region: String, table: String, key: String): String = {
    val dynamoDBClient = new AmazonDynamoDBClient(provider)
    dynamoDBClient.setEndpoint(s"https://dynamodb.${region}.amazonaws.com")
    val dynamoDB = new DynamoDB(dynamoDBClient)
    val item = dynamoDB.getTable(table).getItem("id", key)
    item.getString("json")
  }

  /**
   * Return an enrichment configuration JSON based on the enrichments argument
   * @param provider AWS credentials provider
   * @param enrichmentArgument
   * @return JSON containing configuration for all enrichments
   */
  def extractEnrichmentConfig(
    provider: AWSCredentialsProvider,
    enrichmentArgument: Option[String]
  ): Validation[String, String] = {
    val jsons: Validation[String, List[String]] = enrichmentArgument.map {
      case FilepathRegex(dir) =>
        new java.io.File(dir).listFiles
          .filter(_.getName.endsWith(".json"))
          .map(scala.io.Source.fromFile(_).mkString)
          .toList
          .success
      case DynamoDBRegex(region, table, partialKey) =>
        lookupDynamoDBEnrichments(provider, region, table, partialKey) match {
          case Nil => s"No enrichments found with partial key $partialKey".failure
          case js => js.success
        }
      case other => s"Enrichments argument [$other] must match $regexMsg".failure
    }.getOrElse(Nil.success)

    jsons.map { js =>
      val combinedJson = ("schema" -> "iglu:com.snowplowanalytics.snowplow/enrichments/jsonschema/1-0-0") ~
        ("data" -> js.toList.map(parse(_)))
      compact(combinedJson)
    }
  }

  /**
   * Get a list of enrichment JSONs from DynamoDB
   * @param provider AWS credentials provider
   * @param region DynamoDB region, e.g. "eu-west-1"
   * @param table
   * @param partialKey Primary key prefix, e.g. "enrichments-"
   * @return List of JSONs
   */
  def lookupDynamoDBEnrichments(
    provider: AWSCredentialsProvider,
    region: String, table: String, partialKey: String
  ): List[String] = {
    val dynamoDBClient = new AmazonDynamoDBClient(provider)
    dynamoDBClient.setEndpoint(s"https://dynamodb.${region}.amazonaws.com")

    // Each scan can only return up to 1MB
    // See http://techtraits.com/cloud/nosql/2012/06/27/Amazon-DynamoDB--Understanding-Query-and-Scan-operations/
    @tailrec
    def partialScan(sofar: List[Map[String, String]] = Nil, lastEvaluatedKey: java.util.Map[String, AttributeValue] = null): List[Map[String, String]] = {
      val scanRequest = new ScanRequest().withTableName(table)
      scanRequest.setExclusiveStartKey(lastEvaluatedKey)
      val lastResult = dynamoDBClient.scan(scanRequest)
      val combinedResults = sofar ++ lastResult.getItems.asScala.map(_.asScala.toMap.mapValues(_.getS))
      lastResult.getLastEvaluatedKey match {
        case null => combinedResults
        case startKey => partialScan(combinedResults, startKey)
      }
    }
    val allItems = partialScan(Nil)
    allItems filter { item => item.get("id") match {
        case Some(value) if value.startsWith(partialKey) => true
        case _ => false
      }
    } flatMap(_.get("json"))
  }

  /**
   * Downloads an object from S3 and returns whether
   * or not it was successful.
   * @param provider AWS credentials provider
   * @param uri The URI to reconstruct into a signed S3 URL
   * @param outputFile The file object to write to
   * @return the download result
   */
  def downloadFromS3(provider: AWSCredentialsProvider, uri: URI, outputFile: File): Int = {
    val s3Client = new AmazonS3Client(provider)
    val bucket = uri.getHost
    val key = uri.getPath match { // Need to remove leading '/'
      case s if s.charAt(0) == '/' => s.substring(1)
      case s => s
    }

    try {
      s3Client.getObject(new GetObjectRequest(bucket, key), outputFile)
      0
    } catch {
      case e: Exception => {
        error(s"Error downloading ${uri}: ${e.toString}")
        1
      }
    }
  }
}

// Rigidly load the configuration file here to error when
// the enrichment process starts rather than later.
class KinesisEnrichConfig(config: Config) {
  private val enrich = config.resolve.getConfig("enrich")

  val source = enrich.getString("source") match {
    case "kafka" => Source.Kafka
    case "kinesis" => Source.Kinesis
    case "stdin" => Source.Stdin
    case "test" => Source.Test
    case _ => throw new RuntimeException("enrich.source unknown.")
  }

  val sink = enrich.getString("sink") match {
    case "kafka" => Sink.Kafka
    case "kinesis" => Sink.Kinesis
    case "stdouterr" => Sink.Stdouterr
    case "test" => Sink.Test
    case _ => throw new RuntimeException("enrich.sink unknown.")
  }

  private val aws = enrich.getConfig("aws")
  val accessKey = aws.getString("access-key")
  val secretKey = aws.getString("secret-key")

  private val kafka = enrich.getConfig("kafka")
  val kafkaBrokers = kafka.getString("brokers")

  private val streams = enrich.getConfig("streams")

  private val inStreams = streams.getConfig("in")
  val rawInStream = inStreams.getString("raw")

  private val outStreams = streams.getConfig("out")
  val enrichedOutStream = outStreams.getString("enriched")
  val badOutStream = outStreams.getString("bad")

  val appName = streams.getString("app-name")

  val initialPosition = streams.getString("initial-position")

  val streamRegion = streams.getString("region")
  val streamEndpoint = s"https://kinesis.${streamRegion}.amazonaws.com"

  val maxRecords = if (inStreams.hasPath("maxRecords")) {
    inStreams.getInt("maxRecords")
  } else {
    10000
  }

  val buffer = inStreams.getConfig("buffer")
  val byteLimit = buffer.getInt("byte-limit")
  val recordLimit = buffer.getInt("record-limit")
  val timeLimit = buffer.getInt("time-limit")

  val credentialsProvider = CredentialsLookup.getCredentialsProvider(accessKey, secretKey)

  val backoffPolicy = outStreams.getConfig("backoffPolicy")
  val minBackoff = backoffPolicy.getLong("minBackoff")
  val maxBackoff = backoffPolicy.getLong("maxBackoff")

  val useIpAddressAsPartitionKey = outStreams.hasPath("useIpAddressAsPartitionKey") && outStreams.getBoolean("useIpAddressAsPartitionKey")
}
