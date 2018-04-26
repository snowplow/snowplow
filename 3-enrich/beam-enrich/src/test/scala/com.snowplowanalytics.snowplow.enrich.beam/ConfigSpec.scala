package com.snowplowanalytics.snowplow.enrich.beam

import java.nio.file.Files

import com.spotify.scio.Args
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
        parseEnrichmentRegistry(Some(path)) match {
          case Success(_) => succeed
          case _ => fail()
        }
      }
      "which succeeds if no enrichments dir is given" in {
        parseEnrichmentRegistry(None) match {
          case Success(_) => succeed
          case _ => fail()
        }
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
