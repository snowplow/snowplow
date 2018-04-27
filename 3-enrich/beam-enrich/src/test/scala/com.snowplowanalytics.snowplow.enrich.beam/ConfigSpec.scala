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
package com.snowplowanalytics.snowplow.enrich.beam

import java.nio.file.Files

import com.spotify.scio.Args
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import org.scalatest._
import Matchers._
import scalaz._

import config._
import SpecHelpers._

class ConfigSpec extends FreeSpec {
  "the config object should" - {
    "make an EnrichConfig smart ctor available" - {
      "which fails if --input is not present" in {
        EnrichConfig(Args(Array.empty)) shouldEqual Failure("Missing `input` argument")
      }
      "which fails if --output is not present" in {
        EnrichConfig(Args(Array("--input=i"))) shouldEqual Failure("Missing `output` argument")
      }
      "which fails if --bad is not present" in {
        EnrichConfig(Args(Array("--input=i", "--output=o"))) shouldEqual
          Failure("Missing `bad` argument")
      }
      "which fails if --resolver is not present" in {
        EnrichConfig(Args(Array("--input=i", "--output=o", "--bad=b"))) shouldEqual
          Failure("Missing `resolver` argument")
      }
      "which succeeds otherwise" in {
        EnrichConfig(Args(Array("--input=i", "--output=o", "--bad=b", "--resolver=r"))) shouldEqual
          Success(EnrichConfig("i", "o", "b", "r", None))
      }
      "which succeeds if --enrichments is present" in {
        val args = Args(
          Array("--input=i", "--output=o", "--bad=b", "--resolver=r", "--enrichments=e"))
        EnrichConfig(args) shouldEqual Success(EnrichConfig("i", "o", "b", "r", Some("e")))
      }
    }

    "make a parseResolver function available" - {
      "which fails if there is no resolver file" in {
        parseResolver("doesnt-exist") shouldEqual
          Failure("Iglu resolver configuration file `doesnt-exist` does not exist")
      }
      "which fails if the resolver file is not json" in {
        val path = writeToFile("not-json", "not-json")
        parseResolver(path) match {
          case Failure(e) => e should startWith("Field []: invalid JSON [not-json]")
          case _ => fail()
        }
      }
      "which fails if it's not a resolver" in {
        val path = writeToFile("json", """{"a":2}""")
        parseResolver(path) match {
          case Failure(e) =>
            e should startWith("error: Resolver configuration failed JSON Schema validation")
          case _ => fail()
        }
      }
      "which succeeds if it's a resolver" in {
        val path = writeToFile("resolver", resolverConfig)
        parseResolver(path) match {
          case Success(_) => succeed
          case _ => fail()
        }
      }
    }

    "make a parseEnrichmentRegistry function available" - {
      "which fails if there is no enrichments dir" in {
        parseEnrichmentRegistry(Some("doesnt-exist")) shouldEqual
          Failure("Enrichment directory `doesnt-exist` does not exist")
      }
      "which fails if the contents of the enrichment dir are not json" in {
        val path = writeToFile("not-json", "not-json", "not-json")
        parseEnrichmentRegistry(Some(path)) match {
          case Failure(e) => e should startWith("Field []: invalid JSON [not-json]")
          case _ => fail()
        }
      }
      "which fails if the contents of the enrichment dir are not enrichments" in {
        val path = writeToFile("json", "json", """{"a":2}""")
        parseEnrichmentRegistry(Some(path)) match {
          case Failure(e) =>
            e should startWith("error: NonEmptyList(error: object instance has properties")
          case _ => fail()
        }
      }
      "which succeeds if the contents of the enrichment dir are enrichments" in {
        val path = writeToFile("enrichments", "enrichments", enrichmentConfig)
        parseEnrichmentRegistry(Some(path)) shouldEqual Success(
          ("schema" -> "iglu:com.snowplowanalytics.snowplow/enrichments/jsonschema/1-0-0") ~
          ("data" -> List(parse(enrichmentConfig)))
        )
      }
      "which succeeds if no enrichments dir is given" in {
        parseEnrichmentRegistry(None) shouldEqual Success(
          ("schema" -> "iglu:com.snowplowanalytics.snowplow/enrichments/jsonschema/1-0-0") ~
          ("data" -> List.empty[String])
        )
      }
    }
  }

  private def writeToFile(dir: String, name: String, content: String): String = {
    val d = Files.createTempDirectory(dir)
    Files.write(Files.createTempFile(d.toAbsolutePath, name, ".json"), content.getBytes).toFile
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
