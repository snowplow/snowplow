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

import scala.sys.process._

import cats.Id
import cats.implicits._
import com.snowplowanalytics.iglu.client.Client
import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer, SelfDescribingData}
import com.snowplowanalytics.iglu.core.circe.CirceIgluCodecs._
import com.snowplowanalytics.snowplow.badrows.Processor
import com.snowplowanalytics.snowplow.enrich.common.adapters.AdapterRegistry
import com.snowplowanalytics.snowplow.enrich.common.adapters.registry.RemoteAdapter
import com.snowplowanalytics.snowplow.enrich.common.enrichments.EnrichmentRegistry
import com.snowplowanalytics.snowplow.enrich.common.enrichments.registry.EnrichmentConf
import com.snowplowanalytics.snowplow.enrich.common.utils.JsonUtils
import com.snowplowanalytics.snowplow.scalatracker.Tracker
import com.typesafe.config.ConfigFactory
import io.circe.Json
import io.circe.syntax._
import org.slf4j.LoggerFactory
import pureconfig._
import pureconfig.generic.auto._
import pureconfig.generic.{FieldCoproductHint, ProductHint}

import config._
import model._
import sources.Source
import utils._

/** Interface for the entry point for Stream Enrich. */
trait Enrich {
  protected type EitherS[A] = Either[String, A]

  lazy val log = LoggerFactory.getLogger(getClass())

  val FilepathRegex = "^file:(.+)$".r
  private val regexMsg = "'file:[filename]'"

  implicit val creds: Credentials = NoCredentials

  def run(args: Array[String]): Unit = {
    val trackerSource: Either[String, (Option[Tracker[Id]], Source)] = for {
      config <- parseConfig(args)
      (enrichConfig, resolverArg, enrichmentsArg, forceDownload) = config
      client <- parseClient(resolverArg)
      enrichmentsConf <- parseEnrichmentRegistry(enrichmentsArg, client)(implicitly)
      _ <- cacheFiles(enrichmentsConf, forceDownload)
      tracker = enrichConfig.monitoring.map(c => SnowplowTracking.initializeTracker(c.snowplow))
      enrichmentRegistry <- EnrichmentRegistry.build[Id](enrichmentsConf).value
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
        System.err.println(s"An error occured: $e")
        System.exit(1)
      case Right((tracker, source)) =>
        tracker.foreach(SnowplowTracking.initializeSnowplowTracking)
        source.run()
    }
  }

  /**
   * Source of events
   * @param streamsConfig configuration for the streams
   * @param resolver iglu resolver
   * @param enrichmentRegistry registry of enrichments
   * @param tracker optional tracker
   * @return a validated source, ready to be read from
   */
  def getSource(
    streamsConfig: StreamsConfig,
    client: Client[Id, Json],
    adapterRegistry: AdapterRegistry,
    enrichmentRegistry: EnrichmentRegistry[Id],
    tracker: Option[Tracker[Id]],
    processor: Processor
  ): Either[String, sources.Source]

  implicit def hint[T]: ProductHint[T] = ProductHint[T](ConfigFieldMapping(CamelCase, CamelCase))
  implicit val _ = new FieldCoproductHint[SourceSinkConfig]("enabled")

  /**
   * Parses the configuration from cli arguments
   * @param args cli arguments
   * @return a validated tuple containing the parsed enrich configuration, the resolver argument,
   * the optional enrichments argument and the force download flag
   */
  def parseConfig(
    args: Array[String]
  ): Either[String, (EnrichConfig, String, Option[String], Boolean)] =
    for {
      parsedCliArgs <- parser
        .parse(args, FileConfig())
        .toRight("Error while parsing command line arguments")
      unparsedConfig = Either
        .catchNonFatal(ConfigFactory.parseFile(parsedCliArgs.config).resolve())
        .fold(
          t => t.getMessage.asLeft,
          c =>
            (c, parsedCliArgs.resolver, parsedCliArgs.enrichmentsDir, parsedCliArgs.forceDownload).asRight
        )
      validatedConfig <- unparsedConfig.filterOrElse(
        t => t._1.hasPath("enrich"),
        "No top-level \"enrich\" could be found in the configuration"
      )
      (config, resolverArg, enrichmentsArg, forceDownload) = validatedConfig
      parsedConfig <- Either
        .catchNonFatal(loadConfigOrThrow[EnrichConfig](config.getConfig("enrich")))
        .map(ec => (ec, resolverArg, enrichmentsArg, forceDownload))
        .leftMap(_.getMessage)
    } yield parsedConfig

  /** Cli arguments parser */
  def parser: scopt.OptionParser[FileConfig]
  val localParser =
    new scopt.OptionParser[FileConfig](generated.BuildInfo.name) with FileConfigOptions {
      head(generated.BuildInfo.name, generated.BuildInfo.version)
      help("help")
      version("version")
      configOption()
      localResolverOption()
      localEnrichmentsOption()
      forceCachedFilesDownloadOption()
    }

  /**
   * Retrieve and parse an iglu resolver from the corresponding cli argument value
   * @param resolverArg location of the resolver as a cli argument
a  * @param creds optionally necessary credentials to download the resolver
   * @return a validated iglu resolver
   */
  def parseClient(
    resolverArg: String
  )(
    implicit creds: Credentials
  ): Either[String, Client[Id, Json]] =
    for {
      parsedResolver <- extractResolver(resolverArg)
      json <- JsonUtils.extractJson(parsedResolver)
      client <- Client.parseDefault[Id](json).leftMap(_.toString).value
    } yield client

  /**
   * Return a JSON string based on the resolver argument
   * @param resolverArg location of the resolver
   * @param creds optionally necessary credentials to download the resolver
   * @return JSON from a local file or stored in DynamoDB
   */
  def extractResolver(resolverArg: String)(implicit creds: Credentials): Either[String, String]
  val localResolverExtractor = (resolverArgument: String) =>
    resolverArgument match {
      case FilepathRegex(filepath) =>
        val file = new File(filepath)
        if (file.exists) scala.io.Source.fromFile(file).mkString.asRight
        else "Iglu resolver configuration file \"%s\" does not exist".format(filepath).asLeft
      case _ => s"Resolver argument [$resolverArgument] must match $regexMsg".asLeft
    }

  /**
   * Retrieve and parse an enrichment registry from the corresponding cli argument value
   * @param enrichmentsDirArg location of the enrichments directory as a cli argument
   * @param resolver iglu resolver
   * @param creds optionally necessary credentials to download the enrichments
   * @return a validated enrichment registry
   */
  def parseEnrichmentRegistry(
    enrichmentsDirArg: Option[String],
    client: Client[Id, Json]
  )(
    implicit creds: Credentials
  ): Either[String, List[EnrichmentConf]] =
    for {
      enrichmentConfig <- extractEnrichmentConfigs(enrichmentsDirArg)
      reg <- EnrichmentRegistry.parse(enrichmentConfig, client, false).leftMap(_.toString).toEither
    } yield reg

  /**
   * Return an enrichment configuration JSON based on the enrichments argument
   * @param enrichmentArgument location of the enrichments directory
   * @param creds optionally necessary credentials to download the enrichments
   * @return JSON containing configuration for all enrichments
   */
  def extractEnrichmentConfigs(
    enrichmentArgument: Option[String]
  )(
    implicit creds: Credentials
  ): Either[String, Json]
  val localEnrichmentConfigsExtractor = (enrichmentArgument: Option[String]) => {
    val jsons: Either[String, List[String]] = enrichmentArgument
      .map {
        case FilepathRegex(path) =>
          new File(path).listFiles
            .filter(_.getName.endsWith(".json"))
            .map(scala.io.Source.fromFile(_).mkString)
            .toList
            .asRight
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
   * Download a file locally
   * @param uri of the file to be downloaded
   * @param targetFile local file to download to
   * @param creds optionally necessary credentials to download the file
   * @return the return code of the downloading command
   */
  def download(uri: URI, targetFile: File)(implicit creds: Credentials): Either[String, Int]
  val httpDownloader = (uri: URI, targetFile: File) =>
    uri.getScheme match {
      case "http" | "https" => (uri.toURL #> targetFile).!.asRight
      case s => s"Scheme $s for file $uri not supported".asLeft
    }

  /**
   * Download the IP lookup files locally.
   * @param registry Enrichment registry
   * @param forceDownload CLI flag that invalidates the cached files on each startup
   * @param creds optionally necessary credentials to cache the files
   * @return a list of download command return codes
   */
  def cacheFiles(
    confs: List[EnrichmentConf],
    forceDownload: Boolean
  )(
    implicit creds: Credentials
  ): Either[String, List[Int]] = {
    val filesToCache: List[(URI, String)] = confs.map(_.filesToCache).flatten
    val cleanedFiles: List[(URI, File)] = filesToCache.map {
      case (uri, path) =>
        (
          new URI(uri.toString.replaceAll("(?<!(http:|https:|s3:))//", "/")),
          new File(path)
        )
    }
    val filteredFiles = cleanedFiles.filter {
      case (_, targetFile) =>
        forceDownload || targetFile.length == 0L
    }
    val downloadedFiles: List[Either[String, Int]] = filteredFiles.map {
      case (cleanURI, targetFile) =>
        download(cleanURI, targetFile).flatMap {
          case i if i != 0 => s"Attempt to download $cleanURI to $targetFile failed".asLeft
          case o => o.asRight
        }
    }
    downloadedFiles.sequence[EitherS, Int]
  }

  /**
   *  Sets up the Remote adapters for the ETL
   * @param remoteAdaptersConfig List of configuration per remote adapter
   * @return Mapping of vender-version and the adapter assigned for it
   */
  def prepareRemoteAdapters(remoteAdaptersConfig: Option[List[RemoteAdapterConfig]]) =
    remoteAdaptersConfig match {
      case Some(configList) =>
        configList.map { config =>
          val adapter = new RemoteAdapter(
            config.url,
            config.connectionTimeout,
            config.readTimeout
          )
          (config.vendor, config.version) -> adapter
        }.toMap
      case None => Map.empty[(String, String), RemoteAdapter]
    }
}
