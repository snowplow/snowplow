/*
 * Copyright (c) 2013-2018 Snowplow Analytics Ltd.
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

import scala.collection.JavaConverters._
import scala.util.Try

import com.google.cloud.datastore._
import org.json4s.jackson.JsonMethods._
import org.json4s.JsonDSL._
import scalaz.{Sink => _, Source => _, _}
import Scalaz._

import common.enrichments.EnrichmentRegistry
import config._
import iglu.client.Resolver
import model.{Credentials, StreamsConfig}
import scalatracker.Tracker
import sources.{GooglePubSubSource, Source}

/** The main entry point for Stream Enrich for Google PubSub. */
object GooglePubSubEnrich extends Enrich {

  lazy val UserAgent = s"snowplow/stream-enrich-${generated.BuildInfo.version}"

  private val DatastoreRegex = "^datastore:([^/]*)/([^/]*)$".r
  private val regexMsg = "'file:[filename]' or 'datastore:[kind/key]'"

  def main(args: Array[String]): Unit = run(args)

  override def getSource(
    streamsConfig: StreamsConfig,
    resolver: Resolver,
    enrichmentRegistry: EnrichmentRegistry,
    tracker: Option[Tracker]
  ): Validation[String, Source] =
    GooglePubSubSource.createAndInitialize(streamsConfig, resolver, enrichmentRegistry, tracker)
      .leftMap(_.getMessage)

  override lazy val parser: scopt.OptionParser[FileConfig] =
    new scopt.OptionParser[FileConfig](generated.BuildInfo.name) with FileConfigOptions {
      head(generated.BuildInfo.name, generated.BuildInfo.version)
      help("help")
      version("version")
      configOption()
      opt[String]("resolver").required().valueName("<resolver uri>")
        .text(s"Iglu resolver file, $regexMsg")
        .action((r: String, c: FileConfig) => c.copy(resolver = r))
        .validate(_ match {
          case FilepathRegex(_) | DatastoreRegex(_, _) => success
          case _ => failure(s"Resolver doesn't match accepted uris: $regexMsg")
        })
      opt[String]("enrichments").optional().valueName("<enrichment directory uri>")
        .text(s"Directory of enrichment configuration JSONs, $regexMsg")
        .action((e: String, c: FileConfig) => c.copy(enrichmentsDir = Some(e)))
        .validate(_ match {
          case FilepathRegex(_) | DatastoreRegex(_, _) => success
          case _ => failure(s"Enrichments directory doesn't match accepted uris: $regexMsg")
        })
      forceIpLookupsDownloadOption()
    }

  override def download(uri: URI, targetFile: File)(
      implicit creds: Credentials): Validation[String, Int] =
    httpDownloader(uri, targetFile)

  override def extractResolver(resolverArgument: String)(
      implicit creds: Credentials): Validation[String, String] =
    resolverArgument match {
      case FilepathRegex(filepath) =>
        val file = new File(filepath)
        if (file.exists) scala.io.Source.fromFile(file).mkString.success
        else "Iglu resolver configuration file \"%s\" does not exist".format(filepath).failure
      case DatastoreRegex(kind, key) => lookupDatastoreResolver(kind, key).validation
      case _ => s"Resolver argument [$resolverArgument] must match $regexMsg".failure
    }

  /**
   * Retrieves the resolver from Google Cloud Datastore, assumes the entity key is "json".
   * @param kind the kind of the Entity to retrieve
   * @param keyName the name of the key where the resolver is stored
   * @return the resolver as JSON
   */
  private def lookupDatastoreResolver(kind: String, keyName: String): \/[String, String] = for {
    datastore <- DatastoreOptions.getDefaultInstance().getService().right
    key = datastore.newKeyFactory().setKind(kind).newKey(keyName)
    // weird varargs overloading causes conflicts
    valueOrNull <- utils.toEither(Try(datastore.get(key, List.empty[ReadOption]: _*)))
      .leftMap(_.getMessage)
    value <- Option(valueOrNull).fold("Null value".left[Entity])(_.right[String])
    json <- utils.toEither(Try(value.getString("json"))).leftMap(_.getMessage)
  } yield json

  override def extractEnrichmentConfigs(
      enrichmentArg: Option[String])(implicit creds: Credentials): Validation[String, String] = {
    val jsons: Validation[String, List[String]] = enrichmentArg.map {
      case FilepathRegex(dir) =>
        new File(dir).listFiles
          .filter(_.getName.endsWith(".json"))
          .map(scala.io.Source.fromFile(_).mkString)
          .toList
          .success
      case DatastoreRegex(kind, keyNamePrefix) =>
        lookupDatastoreEnrichments(kind, keyNamePrefix).validation.leftMap(_.getMessage)
          .flatMap {
            case Nil => s"No enrichments found with prefix $keyNamePrefix".failure
            case js => js.success
          }
      case other => s"Enrichments argument [$other] must match $regexMsg".failure
    }.getOrElse(Nil.success)

    jsons.map { js =>
      val combinedJson =
        ("schema" -> "iglu:com.snowplowanalytics.snowplow/enrichments/jsonschema/1-0-0") ~
        ("data" -> js.toList.map(parse(_)))
      compact(combinedJson)
    }
  }

  /**
   * Retrieves the enrichments from Google Cloud Datastore, assumes the entity key is "json".
   * @param kind the kind of the Entity to retrieve
   * @param keyNamePrefix prefix of the keys to retrieve, e.g. "enrichments-"
   * @return the resolver as JSON
   */
  private def lookupDatastoreEnrichments(
      kind: String, keyNamePrefix: String): \/[Throwable, List[String]] = for {
    datastore <- DatastoreOptions.getDefaultInstance().getService().right
    query = Query.newEntityQueryBuilder().setKind(kind).build()
    // weird varargs overloading causes conflicts
    values <- utils.toEither(Try(datastore.run(query, List.empty[ReadOption]: _*)))
      .map(_.asScala.toList)
    jsons <- values
      .filter(v => v.getKey() != null && v.getKey().getName().startsWith(keyNamePrefix))
      .map(v => utils.toEither(Try(v.getString("json"))))
      .sequenceU
  } yield jsons
}
