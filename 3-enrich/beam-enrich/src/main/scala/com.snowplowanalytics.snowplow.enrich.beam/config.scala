/*
 * Copyright (c) 2012-2018 Snowplow Analytics Ltd. All rights reserved.
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
package enrich
package beam

import java.io.File

import scala.io.Source

// import order conflict with json4s
import scalaz._
import Scalaz._
import com.spotify.scio.Args
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

import common.enrichments.EnrichmentRegistry
import common.utils.JsonUtils
import iglu.client.Resolver

object config {
  final case class EnrichConfig(
    jobName: String,
    input: String,
    output: String,
    bad: String,
    resolver: String,
    enrichments: Option[String]
  )
  object EnrichConfig {
    def apply(args: Args): Validation[String, EnrichConfig] = for {
      _ <- if (args.optional("help").isDefined) helpString(configurations).failure else "".success
      l <- configurations.collect {
        case RequiredConfiguration(key, _) => args.optional(key).toSuccess(s"Missing `$key` argument").toValidationNel
      }.sequenceU.leftMap(_.toList.mkString("\n"))
      List(jobName, input, output, bad, resolver) = l
    } yield EnrichConfig(jobName, input, output, bad, resolver, args.optional("enrichments"))

    private val configurations = List(
      RequiredConfiguration("job-name", "Name of the Dataflow job that will be launched"),
      RequiredConfiguration("input", "Name of the subscription to the input topic projects/{project}/subscriptions/{subscription}"),
      RequiredConfiguration("output", "Name of the output topic projects/{project}/topics/{topic}"),
      RequiredConfiguration("bad", "Name of the bad topic projects/{project}/topics/{topic}"),
      RequiredConfiguration("resolver", "Path to the resolver file"),
      OptionalConfiguration("enrichments", "Path to the directory containing the enrichment files")
    )

    private def helpString(configs: List[Configuration]): String =
      "Possible configuration are:\n" +
        configs.map {
          case OptionalConfiguration(key, desc) => s"--$key=VALUE, optional, $desc"
          case RequiredConfiguration(key, desc) => s"--$key=VALUE, required, $desc"
        }.mkString("\n") +
        "\n--help, Display this message" +
        "\nA full list of all the Beam CLI options can be found at: https://cloud.google.com/dataflow/pipelines/specifying-exec-params#setting-other-cloud-pipeline-options"
  }

  sealed trait Configuration {
    def key: String
    def desc: String
  }
  final case class OptionalConfiguration(key: String, desc: String) extends Configuration
  final case class RequiredConfiguration(key: String, desc: String) extends Configuration

  final case class ParsedEnrichConfig(
    input: String,
    output: String,
    bad: String,
    resolver: JValue,
    enrichmentRegistry: JObject
  )

  def parseResolver(resolverPath: String): Validation[String, JValue] = for {
    fileContent <- readResolverFile(resolverPath)
    jsonNode <- JsonUtils.extractJson("", fileContent)
    json = fromJsonNode(jsonNode)
    _ <- Resolver.parse(json).leftMap(_.toList.mkString("\n"))
  } yield json

  private def readResolverFile(path: String): Validation[String, String] = {
    val file = new File(path)
    if (file.exists) Source.fromFile(file).mkString.success
    else s"Iglu resolver configuration file `$path` does not exist".failure
  }

  def parseEnrichmentRegistry(enrichmentsPath: Option[String])(
      implicit resolver: Resolver): Validation[String, JObject] = for {
    fileContents <- readEnrichmentFiles(enrichmentsPath)
    jsons <- fileContents.map(JsonUtils.extractJson("", _)).sequenceU
    combinedJson =
      ("schema" -> "iglu:com.snowplowanalytics.snowplow/enrichments/jsonschema/1-0-0") ~
      ("data" -> jsons.map(fromJsonNode))
    _ <- EnrichmentRegistry.parse(combinedJson, false).leftMap(_.toList.mkString("\n"))
  } yield combinedJson

  private def readEnrichmentFiles(path: Option[String]): Validation[String, List[String]] =
    path.map { p =>
      for {
        files <- Option(new File(p).listFiles)
          .toSuccess(s"Enrichment directory `$p` does not exist")
        read = files
          .filter(_.getName.endsWith(".json"))
          .map(Source.fromFile(_).mkString)
          .toList
      } yield read
    }.getOrElse(Nil.success)
}
