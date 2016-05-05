package com.snowplowanalytics
package snowplow
package enrich
package common
package enrichments
package registry

// Java
import java.util.Locale

// Maven Artifact
import org.apache.maven.artifact.versioning.DefaultArtifactVersion

// Scalaz
import scalaz._
import Scalaz._

// json4s
import org.json4s.JValue

// Iglu
import iglu.client.{
  SchemaCriterion,
  SchemaKey
}
import iglu.client.validation.ProcessingMessageMethods._

// This project
import utils.ScalazJson4sUtils

/**
* Companion object. Lets us create a BrowserLanguageEnrichment
* from a JValue.
*/
object BrowserLanguageEnrichmentConfig extends ParseableEnrichment {

  val supportedSchema = SchemaCriterion("com.snowplowanalytics.snowplow", "browser_language_config", "jsonschema", 1, 0)

  /**
   * Creates an BrowserLanguageEnrichment instance from a JValue.
   *
   * @param config The browser_language enrichment JSON
   * @param schemaKey The SchemaKey provided for the enrichment
   *        Must be a supported SchemaKey for this enrichment
   * @return a configured BrowserLanguageEnrichment instance
   */
  def parse(config: JValue, schemaKey: SchemaKey): ValidatedNelMessage[BrowserLanguageEnrichment.type] =
    isParseable(config, schemaKey).map(_ => BrowserLanguageEnrichment)
}

/**
 * Config for an browser_language enrichment
 *
 * @param browser_language The language string received from the browser
 * @return a language for display
 */
case object BrowserLanguageEnrichment extends Enrichment {
  val version = new DefaultArtifactVersion("0.1.0")

  def convertBrowserLanguage(browser_language: String): String = {
    val locale = Locale.forLanguageTag(browser_language)
    return locale.getDisplayLanguage()
  }
}
