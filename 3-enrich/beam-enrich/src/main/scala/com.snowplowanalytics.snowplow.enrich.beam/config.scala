package com.snowplowanalytics
package snowplow
package enrich
package beam

import java.io.File

import scala.io.Source

import com.spotify.scio.Args
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import scalaz._
import Scalaz._

import common.enrichments.EnrichmentRegistry
import common.utils.JsonUtils
import iglu.client.Resolver

object config {
  final case class EnrichConfig(
    input: String,
    output: String,
    bad: String,
    resolver: String,
    enrichments: Option[String]
  )
  object EnrichConfig {
    def apply(args: Args): Validation[String, EnrichConfig] = for {
      input <- args.optional("input").toSuccess("Missing `input` argument")
      output <- args.optional("output").toSuccess("Missing `output` argument")
      bad <- args.optional("bad").toSuccess("Missing `bad` argument")
      resolver <- args.optional("resolver").toSuccess("Missing `resolver` argument")
    } yield EnrichConfig(input, output, bad, resolver, args.optional("enrichments"))
  }

  final case class ParsedEnrichConfig(
    input: String,
    output: String,
    bad: String,
    resolver: Resolver,
    enrichmentRegistry: EnrichmentRegistry
  )

  def parseResolver(resolverArg: String): Validation[String, Resolver] = for {
    fileContent <- readResolverFile(resolverArg)
    json <- JsonUtils.extractJson("", fileContent)
    resolver <- Resolver.parse(json).leftMap(_.toList.mkString("\n"))
  } yield resolver

  private def readResolverFile(resolverArg: String): Validation[String, String] = {
    val file = new File(resolverArg)
    if (file.exists) Source.fromFile(file).mkString.success
    else s"Iglu resolver configuration file `$resolverArg` does not exist".failure
  }

  def parseEnrichmentRegistry(enrichmentsArg: Option[String])(
      implicit resolver: Resolver): Validation[String, EnrichmentRegistry] = for {
    fileContents <- readEnrichmentFiles(enrichmentsArg)
    jsons <- fileContents.map(JsonUtils.extractJson("", _)).sequenceU
    combinedJson =
      ("schema" -> "iglu:com.snowplowanalytics.snowplow/enrichments/jsonschema/1-0-0") ~
      ("data" -> jsons.map(fromJsonNode))
    registry <- EnrichmentRegistry.parse(combinedJson, false).leftMap(_.toList.mkString("\n"))
  } yield registry

  private def readEnrichmentFiles(
      enrichmentsArg: Option[String]): Validation[String, List[String]] =
    enrichmentsArg.map { arg =>
      for {
        files <- Option(new File(arg).listFiles)
          .toSuccess(s"Enrichment directory `$arg` does not exist")
        read = files
          .filter(_.getName.endsWith(".json"))
          .map(Source.fromFile(_).mkString)
          .toList
      } yield read
    }.getOrElse(Nil.success)
}
