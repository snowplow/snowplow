/*
 * Copyright (c) 2012-2019 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.enrich.beam

import java.nio.file.Files

import com.snowplowanalytics.iglu.core.SelfDescribingData
import com.snowplowanalytics.iglu.core.circe.CirceIgluCodecs._
import com.spotify.scio.Args
import io.circe.Json
import io.circe.syntax._
import org.scalatest._
import org.scalatest.Matchers._

import config._
import SpecHelpers._

class ConfigSpec extends FreeSpec {
  "the config object should" - {
    "make an EnrichConfig smart ctor available" - {
      "which fails if --job-name is not present" in {
        EnrichConfig(Args(Array.empty)) shouldEqual Left(
          "Missing `job-name` argument\n" +
            "Missing `raw` argument\n" +
            "Missing `enriched` argument\n" +
            "Missing `bad` argument\n" +
            "Missing `resolver` argument"
        )
      }
      "which fails if --raw is not present" in {
        EnrichConfig(Args(Array("--job-name=j"))) shouldEqual Left(
          "Missing `raw` argument\n" +
            "Missing `enriched` argument\n" +
            "Missing `bad` argument\n" +
            "Missing `resolver` argument"
        )
      }
      "which fails if --enriched is not present" in {
        EnrichConfig(Args(Array("--job-name=j", "--raw=i"))) shouldEqual Left(
          "Missing `enriched` argument\n" +
            "Missing `bad` argument\n" +
            "Missing `resolver` argument"
        )
      }
      "which fails if --bad is not present" in {
        EnrichConfig(Args(Array("--job-name=j", "--raw=i", "--enriched=o"))) shouldEqual Left(
          "Missing `bad` argument\n" +
            "Missing `resolver` argument"
        )
      }
      "which fails if --resolver is not present" in {
        EnrichConfig(Args(Array("--job-name=j", "--raw=i", "--enriched=o", "--bad=b"))) shouldEqual
          Left("Missing `resolver` argument")
      }
      "which succeeds otherwise" in {
        EnrichConfig(
          Args(Array("--job-name=j", "--raw=i", "--enriched=o", "--bad=b", "--resolver=r"))
        ) shouldEqual
          Right(EnrichConfig("j", "i", "o", "b", None, "r", None))
      }
      "which succeeds if --enrichments is present" in {
        val args = Args(
          Array(
            "--job-name=j",
            "--raw=i",
            "--enriched=o",
            "--bad=b",
            "--resolver=r",
            "--enrichments=e"
          )
        )
        EnrichConfig(args) shouldEqual Right(EnrichConfig("j", "i", "o", "b", None, "r", Some("e")))
      }
      "which succeeds if --pii is present" in {
        val args = Args(
          Array("--job-name=j", "--raw=i", "--enriched=o", "--bad=b", "--pii=p", "--resolver=r")
        )
        EnrichConfig(args) shouldEqual Right(EnrichConfig("j", "i", "o", "b", Some("p"), "r", None))
      }
    }

    "make a parseResolver function available" - {
      "which fails if there is no resolver file" in {
        parseResolver("doesnt-exist") shouldEqual
          Left("Iglu resolver configuration file `doesnt-exist` does not exist")
      }
      "which fails if the resolver file is not json" in {
        val path = writeToFile("not-json", "not-json")
        parseResolver(path) match {
          case Left(e) =>
            e shouldEqual "invalid json: expected null got 'not-js...' (line 1, column 1)"
          case _ => fail()
        }
      }
      "which fails if it's not a resolver" in {
        val path = writeToFile("json", """{"a":2}""")
        parseResolver(path) match {
          case Left(e) => e shouldEqual "schema key is not available"
          case _ => fail()
        }
      }
      "which succeeds if it's a resolver" in {
        val path = writeToFile("resolver", resolverConfig.noSpaces)
        parseResolver(path) match {
          case Right(_) => succeed
          case _ => fail()
        }
      }
    }

    "make a parseEnrichmentRegistry function available" - {
      "which fails if there is no enrichments dir" in {
        parseEnrichmentRegistry(Some("doesnt-exist"), SpecHelpers.client) shouldEqual
          Left("Enrichment directory `doesnt-exist` does not exist")
      }
      "which fails if the contents of the enrichment dir are not json" in {
        val path = writeToFile("not-json", "not-json", "not-json")
        parseEnrichmentRegistry(Some(path), SpecHelpers.client) match {
          case Left(e) =>
            e shouldEqual "invalid json: expected null got 'not-js...' (line 1, column 1)"
          case _ => fail()
        }
      }
      "which fails if the contents of the enrichment dir are not enrichments" in {
        val path = writeToFile("json", "json", """{"a":2}""")
        parseEnrichmentRegistry(Some(path), SpecHelpers.client) match {
          case Left(e) =>
            e shouldEqual """{"error":"ValidationError","dataReports":[{"message":"$[0].schema: is missing but it is required","path":"$[0]","keyword":"required","targets":["schema"]},{"message":"$[0].data: is missing but it is required","path":"$[0]","keyword":"required","targets":["data"]},{"message":"$[0].a: is not defined in the schema and the schema does not allow additional properties","path":"$[0]","keyword":"additionalProperties","targets":["a"]}]}"""
          case _ => fail()
        }
      }
      "which succeeds if the contents of the enrichment dir are enrichments" in {
        val path = writeToFile("enrichments", "enrichments", enrichmentConfig.noSpaces)
        parseEnrichmentRegistry(Some(path), SpecHelpers.client) shouldEqual Right(
          SelfDescribingData(
            SpecHelpers.enrichmentsSchemaKey,
            Json.arr(enrichmentConfig)
          ).asJson
        )
      }
      "which succeeds if no enrichments dir is given" in {
        parseEnrichmentRegistry(None, SpecHelpers.client) shouldEqual Right(
          SelfDescribingData(
            SpecHelpers.enrichmentsSchemaKey,
            Json.arr()
          ).asJson
        )
      }
    }
  }

  private def writeToFile(
    dir: String,
    name: String,
    content: String
  ): String = {
    val d = Files.createTempDirectory(dir)
    Files
      .write(Files.createTempFile(d.toAbsolutePath, name, ".json"), content.getBytes)
      .toFile
      .deleteOnExit()
    val f = d.toFile()
    f.deleteOnExit()
    f.getAbsolutePath
  }

  private def writeToFile(name: String, content: String): String = {
    val f = Files.write(Files.createTempFile(name, ".json"), content.getBytes).toFile
    f.deleteOnExit()
    f.getAbsolutePath
  }
}
