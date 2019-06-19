/*
 * Copyright (c) 2013-2019 Snowplow Analytics Ltd.
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
package com.snowplowanalytics.snowplow.enrich.stream

import java.io.File
import java.net.URI

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.io.Source
import scala.sys.process._

import cats.Id
import cats.implicits._
import com.amazonaws.auth._
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.services.dynamodbv2.model.{AttributeValue, ScanRequest}
import com.amazonaws.services.dynamodbv2.document.{DynamoDB, Item}
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.s3.model.GetObjectRequest
import com.snowplowanalytics.iglu.client.Client
import com.snowplowanalytics.iglu.core._
import com.snowplowanalytics.iglu.core.circe.CirceIgluCodecs._
import com.snowplowanalytics.snowplow.badrows.Processor
import com.snowplowanalytics.snowplow.enrich.common.adapters.AdapterRegistry
import com.snowplowanalytics.snowplow.enrich.common.enrichments.EnrichmentRegistry
import com.snowplowanalytics.snowplow.enrich.common.utils.JsonUtils
import com.snowplowanalytics.snowplow.scalatracker.Tracker
import io.circe.Json
import io.circe.syntax._

import config._
import model.{AWSCredentials, Credentials, Kinesis, NoCredentials, StreamsConfig}
import sources.KinesisSource

/** The main entry point for Stream Enrich for Kinesis. */
object KinesisEnrich extends Enrich {

  val DynamoDBRegex = "^dynamodb:([^/]*)/([^/]*)/([^/]*)$".r
  private val regexMsg = "'file:[filename]' or 'dynamodb:[region/table/key]'"

  def main(args: Array[String]): Unit = {
    val trackerSource = for {
      config <- parseConfig(args)
      (enrichConfig, resolverArg, enrichmentsArg, forceDownload) = config
      creds <- enrichConfig.streams.sourceSink match {
        case c: Kinesis => c.aws.asRight
        case _ => "Configured source/sink is not Kinesis".asLeft
      }
      client <- parseClient(resolverArg)(creds)
      enrichmentsConf <- parseEnrichmentRegistry(enrichmentsArg, client)(creds)
      _ <- cacheFiles(enrichmentsConf, forceDownload)(creds)
      enrichmentRegistry <- EnrichmentRegistry.build[Id](enrichmentsConf).value
      tracker = enrichConfig.monitoring.map(c => SnowplowTracking.initializeTracker(c.snowplow))
      adapterRegistry = new AdapterRegistry(prepareRemoteAdapters(enrichConfig.remoteAdapters))
      processor = Processor(generated.BuildInfo.name, generated.BuildInfo.version)
      source <- getSource(
        enrichConfig.streams,
        client,
        adapterRegistry,
        enrichmentRegistry,
        tracker,
        processor
      )
    } yield (tracker, source)

    trackerSource match {
      case Left(e) =>
        System.err.println(e)
        System.exit(1)
      case Right((tracker, source)) =>
        tracker.foreach(SnowplowTracking.initializeSnowplowTracking)
        source.run()
    }
  }

  override def getSource(
    streamsConfig: StreamsConfig,
    client: Client[Id, Json],
    adapterRegistry: AdapterRegistry,
    enrichmentRegistry: EnrichmentRegistry[Id],
    tracker: Option[Tracker[Id]],
    processor: Processor
  ): Either[String, sources.Source] =
    KinesisSource.createAndInitialize(
      streamsConfig,
      client,
      adapterRegistry,
      enrichmentRegistry,
      tracker,
      processor
    )

  override lazy val parser: scopt.OptionParser[FileConfig] =
    new scopt.OptionParser[FileConfig](generated.BuildInfo.name) with FileConfigOptions {
      head(generated.BuildInfo.name, generated.BuildInfo.version)
      help("help")
      version("version")
      configOption()
      opt[String]("resolver")
        .required()
        .valueName("<resolver uri>")
        .text(s"Iglu resolver file, $regexMsg")
        .action((r: String, c: FileConfig) => c.copy(resolver = r))
        .validate(_ match {
          case FilepathRegex(_) | DynamoDBRegex(_, _, _) => success
          case _ => failure(s"Resolver doesn't match accepted uris: $regexMsg")
        })
      opt[String]("enrichments")
        .optional()
        .valueName("<enrichment directory uri>")
        .text(s"Directory of enrichment configuration JSONs, $regexMsg")
        .action((e: String, c: FileConfig) => c.copy(enrichmentsDir = Some(e)))
        .validate(_ match {
          case FilepathRegex(_) | DynamoDBRegex(_, _, _) => success
          case _ => failure(s"Enrichments directory doesn't match accepted uris: $regexMsg")
        })
      forceCachedFilesDownloadOption()
    }

  override def download(
    uri: URI,
    targetFile: File
  )(
    implicit creds: Credentials
  ): Either[String, Int] =
    uri.getScheme match {
      case "http" | "https" => (uri.toURL #> targetFile).!.asRight
      case "s3" =>
        for {
          provider <- getProvider(creds)
          downloadResult <- downloadFromS3(provider, uri, targetFile).leftMap(_.getMessage)
        } yield downloadResult
      case s => s"Scheme $s for file $uri not supported".asLeft
    }

  /**
   * Downloads an object from S3 and returns whether or not it was successful.
   * @param uri The URI to reconstruct into a signed S3 URL
   * @param targetFile The file object to write to
   * @param creds necessary credentials to download from S3
   * @return the download result
   */
  private def downloadFromS3(
    provider: AWSCredentialsProvider,
    uri: URI,
    targetFile: File
  ): Either[Throwable, Int] = {
    val s3Client = AmazonS3ClientBuilder
      .standard()
      .withCredentials(provider)
      .build()
    val bucket = uri.getHost
    val key = uri.getPath match { // Need to remove leading '/'
      case s if s.length > 0 && s.charAt(0) == '/' => s.substring(1)
      case s => s
    }

    Either.catchNonFatal {
      s3Client.getObject(new GetObjectRequest(bucket, key), targetFile)
      0
    }
  }

  override def extractResolver(
    resolverArgument: String
  )(
    implicit creds: Credentials
  ): Either[String, String] =
    resolverArgument match {
      case FilepathRegex(filepath) =>
        val file = new File(filepath)
        if (file.exists) Source.fromFile(file).mkString.asRight
        else "Iglu resolver configuration file \"%s\" does not exist".format(filepath).asLeft
      case DynamoDBRegex(region, table, key) =>
        for {
          provider <- getProvider(creds)
          resolver <- lookupDynamoDBResolver(provider, region, table, key)
        } yield resolver
      case _ => s"Resolver argument [$resolverArgument] must match $regexMsg".asLeft
    }

  /**
   * Fetch configuration from DynamoDB, assumes the primary key is "id" and the configuration key is
   * "json"
   * @param provider aws credentials provider
   * @param region DynamoDB region, e.g. "eu-west-1"
   * @param table DynamoDB table containing the resolver
   * @param key The value of the primary key for the configuration
   * @return The JSON stored in DynamoDB
   */
  private def lookupDynamoDBResolver(
    provider: AWSCredentialsProvider,
    region: String,
    table: String,
    key: String
  ): Either[String, String] = {
    val dynamoDBClient = AmazonDynamoDBClientBuilder
      .standard()
      .withCredentials(provider)
      .withEndpointConfiguration(new EndpointConfiguration(getDynamodbEndpoint(region), region))
      .build()
    val dynamoDB = new DynamoDB(dynamoDBClient)
    for {
      // getTable doesn't involve any IO apparently so it's safe to chain
      item <- Option(dynamoDB.getTable(table).getItem("id", key))
        .fold(s"Key $key doesn't exist in DynamoDB table $table".asLeft[Item])(_.asRight[String])
      json <- Option(item.getString("json"))
        .fold(s"""Field "json" not found at key $key in DynamoDB table $table""".asLeft[String])(
          _.asRight[String]
        )
    } yield json
  }

  override def extractEnrichmentConfigs(
    enrichmentArg: Option[String]
  )(
    implicit creds: Credentials
  ): Either[String, Json] = {
    val jsons: Either[String, List[String]] = enrichmentArg
      .map {
        case FilepathRegex(dir) =>
          new File(dir).listFiles
            .filter(_.getName.endsWith(".json"))
            .map(scala.io.Source.fromFile(_).mkString)
            .toList
            .asRight
        case DynamoDBRegex(region, table, keyNamePrefix) =>
          for {
            provider <- getProvider(creds)
            enrichmentList = lookupDynamoDBEnrichments(provider, region, table, keyNamePrefix)
            enrichments <- enrichmentList match {
              case Nil => s"No enrichments found with prefix $keyNamePrefix".asLeft
              case js => js.asRight
            }
          } yield enrichments
        case other => s"Enrichments argument [$other] must match $regexMsg".asLeft
      }
      .getOrElse(Nil.asRight)

    val schemaKey = SchemaKey(
      "com.snowplowanalytics.snowplow",
      "enrichments",
      "jsonschema",
      SchemaVer.Full(1, 0, 0)
    )

    jsons
      .flatMap(_.map(JsonUtils.extractJson).sequence[EitherS, Json])
      .map(jsons => SelfDescribingData[Json](schemaKey, Json.fromValues(jsons)).asJson)
  }

  /**
   * Get a list of enrichment JSONs from DynamoDB
   * @param provider aws credentials provider
   * @param region DynamoDB region, e.g. "eu-west-1"
   * @param table
   * @param keyNamePrefix Primary key prefix, e.g. "enrichments-"
   * @return List of JSONs
   */
  private def lookupDynamoDBEnrichments(
    provider: AWSCredentialsProvider,
    region: String,
    table: String,
    keyNamePrefix: String
  ): List[String] = {
    val dynamoDBClient = AmazonDynamoDBClientBuilder
      .standard()
      .withCredentials(provider)
      .withEndpointConfiguration(new EndpointConfiguration(getDynamodbEndpoint(region), region))
      .build()

    // Each scan can only return up to 1MB
    // See http://techtraits.com/cloud/nosql/2012/06/27/Amazon-DynamoDB--Understanding-Query-and-Scan-operations/
    @tailrec
    def partialScan(
      sofar: List[Map[String, String]],
      lastEvaluatedKey: java.util.Map[String, AttributeValue] = null
    ): List[Map[String, String]] = {
      val scanRequest = new ScanRequest().withTableName(table)
      scanRequest.setExclusiveStartKey(lastEvaluatedKey)
      val lastResult = dynamoDBClient.scan(scanRequest)
      val combinedResults = sofar ++
        lastResult.getItems.asScala.map(_.asScala.toMap.mapValues(_.getS))
      lastResult.getLastEvaluatedKey match {
        case null => combinedResults
        case startKey => partialScan(combinedResults, startKey)
      }
    }
    val allItems = partialScan(Nil)
    allItems
      .filter { item =>
        item.get("id") match {
          case Some(value) if value.startsWith(keyNamePrefix) => true
          case _ => false
        }
      }
      .flatMap(_.get("json"))
  }

  def getProvider(creds: Credentials): Either[String, AWSCredentialsProvider] = {
    def isDefault(key: String): Boolean = key == "default"
    def isIam(key: String): Boolean = key == "iam"
    def isEnv(key: String): Boolean = key == "env"

    for {
      awsCreds <- creds match {
        case NoCredentials => "No AWS credentials provided".asLeft
        case c: AWSCredentials => c.asRight
      }
      provider <- awsCreds match {
        case AWSCredentials(a, s) if isDefault(a) && isDefault(s) =>
          new DefaultAWSCredentialsProviderChain().asRight
        case AWSCredentials(a, s) if isDefault(a) || isDefault(s) =>
          "accessKey and secretKey must both be set to 'default' or neither".asLeft
        case AWSCredentials(a, s) if isIam(a) && isIam(s) =>
          InstanceProfileCredentialsProvider.getInstance().asRight
        case AWSCredentials(a, s) if isIam(a) && isIam(s) =>
          "accessKey and secretKey must both be set to 'iam' or neither".asLeft
        case AWSCredentials(a, s) if isEnv(a) && isEnv(s) =>
          new EnvironmentVariableCredentialsProvider().asRight
        case AWSCredentials(a, s) if isEnv(a) || isEnv(s) =>
          "accessKey and secretKey must both be set to 'env' or neither".asLeft
        case AWSCredentials(a, s) =>
          new AWSStaticCredentialsProvider(new BasicAWSCredentials(a, s)).asRight
      }
    } yield provider
  }

  private def getDynamodbEndpoint(region: String): String =
    region match {
      case cn @ "cn-north-1" => s"https://dynamodb.$cn.amazonaws.com.cn"
      case _ => s"https://dynamodb.$region.amazonaws.com"
    }
}
