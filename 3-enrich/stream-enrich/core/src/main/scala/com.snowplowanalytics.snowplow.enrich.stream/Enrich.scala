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
package com.snowplowanalytics
package snowplow
package enrich
package stream

import java.io.File
import java.net.URI

import scala.io.Source
import scala.util.Try
import scala.sys.process._
import com.typesafe.config.ConfigFactory
import org.json4s.jackson.JsonMethods._
import org.json4s.JsonDSL._
import org.slf4j.LoggerFactory
import pureconfig._
import scalaz.{Sink => _, Source => _, _}
import Scalaz._

import common.adapters.AdapterRegistry
import common.adapters.registry.RemoteAdapter
import common.enrichments.EnrichmentRegistry
import common.utils.JsonUtils
import config._
import iglu.client.Resolver
import model._
import scalatracker.Tracker

/** Interface for the entry point for Stream Enrich. */
trait Enrich {

  lazy val log = LoggerFactory.getLogger(getClass())

  val FilepathRegex = "^file:(.+)$".r
  private val regexMsg = "'file:[filename]'"

  implicit val creds: Credentials = NoCredentials

  def run(args: Array[String]): Unit = {
    val trackerSource = for {
      config <- parseConfig(args).validation
      (enrichConfig, resolverArg, enrichmentsArg, forceDownload) = config
      resolver <- parseResolver(resolverArg)
      enrichmentRegistry <- parseEnrichmentRegistry(enrichmentsArg)(resolver, implicitly)
      adapterRegistry = new AdapterRegistry(prepareRemoteAdapters(enrichConfig.remoteAdapters))
      _ <- cacheFiles(enrichmentRegistry, forceDownload)
      tracker = enrichConfig.monitoring.map(c => SnowplowTracking.initializeTracker(c.snowplow))
      source <- getSource(enrichConfig.streams, resolver, adapterRegistry, enrichmentRegistry, tracker)
    } yield (tracker, source)

    trackerSource match {
      case Failure(e) =>
        System.err.println(s"An error occured: $e")
        System.exit(1)
      case Success((tracker, source)) =>
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
    resolver: Resolver,
    adapterRegistry: AdapterRegistry,
    enrichmentRegistry: EnrichmentRegistry,
    tracker: Option[Tracker]
  ): Validation[String, sources.Source]

  /**
   * Parses the configuration from cli arguments
   * @param args cli arguments
   * @return a validated tuple containing the parsed enrich configuration, the resolver argument,
   * the optional enrichments argument and the force download flag
   */
  def parseConfig(
    args: Array[String]
  ): \/[String, (EnrichConfig, String, Option[String], Boolean)] = {
    implicit def hint[T] = ProductHint[T](ConfigFieldMapping(CamelCase, CamelCase))
    implicit val sourceSinkConfigHint = new FieldCoproductHint[SourceSinkConfig]("enabled")
    for {
      parsedCliArgs <- \/.fromEither(
        parser.parse(args, FileConfig()).toRight("Error while parsing command line arguments")
      )
      unparsedConfig = utils.fold(Try(ConfigFactory.parseFile(parsedCliArgs.config).resolve()))(
        t => t.getMessage.left,
        c =>
          (c, parsedCliArgs.resolver, parsedCliArgs.enrichmentsDir, parsedCliArgs.forceDownload).right
      )
      validatedConfig <- utils.filterOrElse(unparsedConfig)(
        t => t._1.hasPath("enrich"),
        "No top-level \"enrich\" could be found in the configuration"
      )
      (config, resolverArg, enrichmentsArg, forceDownload) = validatedConfig
      parsedConfig <- utils
        .toEither(Try(loadConfigOrThrow[EnrichConfig](config.getConfig("enrich"))))
        .map(ec => (ec, resolverArg, enrichmentsArg, forceDownload))
        .leftMap(_.getMessage)
    } yield parsedConfig
  }

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
  def parseResolver(
    resolverArg: String
  )(
    implicit creds: Credentials
  ): Validation[String, Resolver] =
    for {
      parsedResolver <- extractResolver(resolverArg)
      json <- JsonUtils.extractJson("", parsedResolver)
      resolver <- Resolver.parse(json).leftMap(_.toString)
    } yield resolver

  /**
   * Return a JSON string based on the resolver argument
   * @param resolverArg location of the resolver
   * @param creds optionally necessary credentials to download the resolver
   * @return JSON from a local file or stored in DynamoDB
   */
  def extractResolver(resolverArg: String)(implicit creds: Credentials): Validation[String, String]
  val localResolverExtractor = (resolverArgument: String) =>
    resolverArgument match {
      case FilepathRegex(filepath) =>
        val file = new File(filepath)
        if (file.exists) Source.fromFile(file).mkString.success
        else "Iglu resolver configuration file \"%s\" does not exist".format(filepath).failure
      case _ => s"Resolver argument [$resolverArgument] must match $regexMsg".failure
    }

  /**
   * Retrieve and parse an enrichment registry from the corresponding cli argument value
   * @param enrichmentsDirArg location of the enrichments directory as a cli argument
   * @param resolver iglu resolver
   * @param creds optionally necessary credentials to download the enrichments
   * @return a validated enrichment registry
   */
  def parseEnrichmentRegistry(
    enrichmentsDirArg: Option[String]
  )(
    implicit resolver: Resolver,
    creds: Credentials
  ): Validation[String, EnrichmentRegistry] =
    for {
      enrichmentConfig <- extractEnrichmentConfigs(enrichmentsDirArg)
      registryConfig <- JsonUtils.extractJson("", enrichmentConfig)
      reg <- EnrichmentRegistry.parse(fromJsonNode(registryConfig), false).leftMap(_.toString)
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
  ): Validation[String, String]
  val localEnrichmentConfigsExtractor = (enrichmentArgument: Option[String]) => {
    val jsons: Validation[String, List[String]] = enrichmentArgument
      .map {
        case FilepathRegex(path) =>
          new File(path).listFiles
            .filter(_.getName.endsWith(".json"))
            .map(scala.io.Source.fromFile(_).mkString)
            .toList
            .success
        case other => s"Enrichments argument [$other] must match $regexMsg".failure
      }
      .getOrElse(Nil.success)

    jsons.map { js =>
      val combinedJson =
        ("schema" -> "iglu:com.snowplowanalytics.snowplow/enrichments/jsonschema/1-0-0") ~
          ("data" -> js.toList.map(parse(_)))
      compact(combinedJson)
    }
  }

  /**
   * Download a file locally
   * @param uri of the file to be downloaded
   * @param targetFile local file to download to
   * @param creds optionally necessary credentials to download the file
   * @return the return code of the downloading command
   */
  def download(uri: URI, targetFile: File)(implicit creds: Credentials): Validation[String, Int]
  val httpDownloader = (uri: URI, targetFile: File) =>
    uri.getScheme match {
      case "http" | "https" => (uri.toURL #> targetFile).!.success
      case s => s"Scheme $s for file $uri not supported".failure
    }

  /**
   * Download the IP lookup files locally.
   * @param registry Enrichment registry
   * @param forceDownload CLI flag that invalidates the cached files on each startup
   * @param creds optionally necessary credentials to cache the files
   * @return a list of download command return codes
   */
  def cacheFiles(
    registry: EnrichmentRegistry,
    forceDownload: Boolean
  )(
    implicit creds: Credentials
  ): ValidationNel[String, List[Int]] =
    registry.filesToCache
      .map {
        case (uri, path) =>
          (
            new java.net.URI(uri.toString.replaceAll("(?<!(http:|https:|s3:))//", "/")),
            new File(path)
          )
      }
      .filter { case (_, targetFile) => forceDownload || targetFile.length == 0L }
      .map {
        case (cleanURI, targetFile) =>
          download(cleanURI, targetFile).flatMap {
            case i if i != 0 => s"Attempt to download $cleanURI to $targetFile failed".failure
            case o => o.success
          }.toValidationNel
      }
      .sequenceU

  /**
    *  Sets up the Remote adapters for the ETL
    * @param remoteAdaptersConfig List of configuration per remote adapter
    * @return Mapping of vender-version and the adapter assigned for it
    */
  def prepareRemoteAdapters(remoteAdaptersConfig: Option[List[RemoteAdapterConfig]]) = {
    remoteAdaptersConfig match {
      case Some(configList) => configList.map { config =>
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
}
