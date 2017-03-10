/*
 * Copyright (c) 2013-2017 Snowplow Analytics Ltd.
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
package com.snowplowanalytics
package snowplow.enrich
package stream

import java.io.File
import java.net.URI

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.io.Source
import scala.util.Try
import scala.util.control.NonFatal
import sys.process._

import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.services.dynamodbv2.model.ScanRequest
import com.amazonaws.services.dynamodbv2.model.AttributeValue
import com.amazonaws.services.dynamodbv2.document.DynamoDB
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.s3.model.GetObjectRequest
import com.typesafe.config.ConfigFactory
import org.json4s.jackson.JsonMethods._
import org.json4s.JsonDSL._
import org.slf4j.LoggerFactory
import pureconfig._
import scalaz.{Sink => _, Source => _, _}
import Scalaz._

import common.FatalEtlError
import common.enrichments.EnrichmentRegistry
import common.utils.JsonUtils
import iglu.client.Resolver
import model._
import sources._

/**
 * The main entry point for Stream Enrich.
 */
object EnrichApp {

  lazy val log = LoggerFactory.getLogger(getClass())

  val FilepathRegex = "^file:(.+)$".r
  val DynamoDBRegex = "^dynamodb:([^/]*)/([^/]*)/([^/]*)$".r
  val regexMsg = "'file:[filename]' or 'dynamodb:[region/table/key]'"

  case class FileConfig(
    config: File = new File("."),
    resolver: String = "",
    enrichmentsDir: Option[String] = None,
    forceDownload: Boolean = false
  )

  def main(args: Array[String]): Unit = {
    val parser = new scopt.OptionParser[FileConfig](generated.Settings.name) {
      head(generated.Settings.name, generated.Settings.version)
      help("help")
      version("version")
      opt[File]("config").required().valueName("<filename>")
        .action((f: File, c: FileConfig) => c.copy(config = f))
        .validate(f =>
          if (f.exists) success
          else failure(s"Configuration file $f does not exist")
        )
      opt[String]("resolver").required().valueName("<resolver uri>")
        .text(s"Iglu resolver file, $regexMsg")
        .action((r: String, c: FileConfig) => c.copy(resolver = r))
        .validate(_ match {
          case FilepathRegex(_) | DynamoDBRegex(_, _, _) => success
          case _ => failure(s"Resolver doesn't match accepted uris: $regexMsg")
        })
      opt[String]("enrichments").optional().valueName("<enrichment directory uri>")
        .text(s"Directory of enrichment configuration JSONs, $regexMsg")
        .action((e: String, c: FileConfig) => c.copy(enrichmentsDir = Some(e)))
        .validate(_ match {
          case FilepathRegex(_) | DynamoDBRegex(_, _, _) => success
          case _ => failure(s"Enrichments directory doesn't match accepted uris: $regexMsg")
        })
      opt[Unit]("force-ip-lookups-download")
        .text("Invalidate the cached IP lookup files and download them anew")
        .action((_, c) => c.copy(forceDownload = true))
    }

    implicit def hint[T] = ProductHint[T](ConfigFieldMapping(CamelCase, CamelCase))
    val conf: Either[String, (EnrichConfig, String, Option[String], Boolean)] = utils.filterOrElse(
      parser.parse(args, FileConfig())
        .toRight("Error while parsing command-line arguments")
        .right.flatMap { fc =>
          utils.fold(Try(ConfigFactory.parseFile(fc.config).resolve()))(
            t => Left(t.getMessage), c => Right((c, fc.resolver, fc.enrichmentsDir, fc.forceDownload)))
        }
    )(t => t._1.hasPath("enrich"), "No top-level \"enrich\" could be found in the configuration")
      .flatMap { case (config, resolver, enrichments, forceDownload) =>
        utils.fold(Try(loadConfigOrThrow[EnrichConfig](config.getConfig("enrich"))))(
            t => Left(t.getMessage), Right(_))
          .right.map(ec => (ec, resolver, enrichments, forceDownload))
      }

    conf match {
      case Left(e) =>
        System.err.println(s"configuration error: $e")
        System.exit(1)
      case Right((enrichConfig, resolver, enrichments, forceDownload)) =>
        run(enrichConfig, resolver, enrichments, forceDownload)
    }
  }

  def run(
    ec: EnrichConfig,
    resolver: String,
    enrichmentsDir: Option[String],
    forceDownload: Boolean
  ): Unit = {
    val tracker = ec.monitoring.map(c => SnowplowTracking.initializeTracker(c.snowplow))

    val provider = ec.aws.provider
    implicit val igluResolver: Resolver = (for {
      parsedResolver <- extractResolver(provider, resolver)
      json <- JsonUtils.extractJson("", parsedResolver)
      resolver <- Resolver.parse(json).leftMap(_.toString)
    } yield resolver) fold (
      e => throw new RuntimeException(e),
      s => s
    )

    val registry: EnrichmentRegistry = (for {
      enrichmentConfig <- extractEnrichmentConfig(provider, enrichmentsDir)
      registryConfig <- JsonUtils.extractJson("", enrichmentConfig)
      reg <- EnrichmentRegistry.parse(fromJsonNode(registryConfig), false).leftMap(_.toString)
    } yield reg) fold (
      e => throw new RuntimeException(e),
      s => s
    )

    cacheFiles(registry, forceDownload, provider).fold(fs => throw FatalEtlError(fs), _ => ())

    val source = ec.sourceType match {
      case KafkaSource => new KafkaSource(ec, igluResolver, registry, tracker)
      case KinesisSource => new KinesisSource(ec, igluResolver, registry, tracker)
      case StdinSource => new StdinSource(ec, igluResolver, registry, tracker)
      case NsqSource => new NsqSource(ec, igluResolver, registry, tracker)
    }

    tracker.foreach(SnowplowTracking.initializeSnowplowTracking)

    source.run
  }

  /**
   * Download the IP lookup files locally.
   * @param registry Enrichment registry
   * @param forceDownload CLI flag that invalidates the cached files on each startup
   * @param provider AWS credentials provider necessary to download files from S3
   * @return a list of failures
   */
  def cacheFiles(
    registry: EnrichmentRegistry,
    forceDownload: Boolean,
    provider: => AWSCredentialsProvider
  ): ValidationNel[String, List[Int]] =
    registry.getIpLookupsEnrichment
      .map(_.dbsToCache)
      .toList.flatten
      .map { case (uri, path) =>
        (new java.net.URI(uri.toString.replaceAll("(?<!(http:|https:|s3:))//", "/")),
          new File(path))
      }
      .filter { case (_, targetFile) => forceDownload || targetFile.length == 0L }
      .map { case (cleanURI, targetFile) =>
        val downloadResult = cleanURI.getScheme match {
          case "http" | "https" => (cleanURI.toURL #> targetFile).!.success
          case "s3"             => downloadFromS3(provider, cleanURI, targetFile).success
          case s                => s"Scheme $s for file $cleanURI not supported".failure
        }
        downloadResult
          .flatMap {
            case i if i != 0 => s"Attempt to download $cleanURI to $targetFile failed".failure
            case o => o.success
          }
          .toValidationNel
      }
      .sequenceU

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
    case FilepathRegex(filepath) =>
      val file = new File(filepath)
      if (file.exists) {
        Source.fromFile(file).mkString.success
      } else {
        "Iglu resolver configuration file \"%s\" does not exist".format(filepath).failure
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
    val dynamoDBClient = AmazonDynamoDBClientBuilder
      .standard()
      .withCredentials(provider)
      .withEndpointConfiguration(new EndpointConfiguration(getDynamodbEndpoint(region), region))
      .build()
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
        new File(dir).listFiles
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
    val dynamoDBClient = AmazonDynamoDBClientBuilder
      .standard()
      .withCredentials(provider)
      .withEndpointConfiguration(new EndpointConfiguration(getDynamodbEndpoint(region), region))
      .build()

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
   * Downloads an object from S3 and returns whether or not it was successful.
   * @param provider AWS credentials provider
   * @param uri The URI to reconstruct into a signed S3 URL
   * @param outputFile The file object to write to
   * @return the download result
   */
  def downloadFromS3(provider: AWSCredentialsProvider, uri: URI, outputFile: File): Int = {
    val s3Client = AmazonS3ClientBuilder
      .standard()
      .withCredentials(provider)
      .build()
    val bucket = uri.getHost
    val key = uri.getPath match { // Need to remove leading '/'
      case s if s.charAt(0) == '/' => s.substring(1)
      case s => s
    }

    try {
      s3Client.getObject(new GetObjectRequest(bucket, key), outputFile)
      0
    } catch {
      case NonFatal(e) =>
        log.error(s"Error downloading $uri: ${e.getMessage}")
        1
    }
  }

  private def getDynamodbEndpoint(region: String): String =
    region match {
      case cn@"cn-north-1" => s"https://dynamodb.$cn.amazonaws.com.cn"
      case _ => s"https://dynamodb.$region.amazonaws.com"
    }
}
