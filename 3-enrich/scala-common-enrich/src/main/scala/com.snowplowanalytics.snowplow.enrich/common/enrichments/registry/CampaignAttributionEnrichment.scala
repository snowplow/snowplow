/*
 * Copyright (c) 2014 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
 */
package com.snowplowanalytics
package snowplow
package enrich
package common
package enrichments
package registry

// Java
import java.net.URI

// Scala
import scala.collection.JavaConversions._
import scala.reflect.BeanProperty

// Utils
import org.apache.http.client.utils.URLEncodedUtils

// Maven Artifact
import org.apache.maven.artifact.versioning.DefaultArtifactVersion

// Scalaz
import scalaz._
import Scalaz._

// json4s
import org.json4s.JValue

// Iglu
import iglu.client.SchemaKey

// This project
import utils.{ConversionUtils => CU}
import utils.MapTransformer.{
  SourceMap,
  TransformFunc
}
import utils.ScalazJson4sUtils

/**
 * Companion object. Lets us create a
 * CampaignAttributionEnrichment from a JValue
 */
object CampaignAttributionEnrichment extends ParseableEnrichment {

  val supportedSchemaKey = SchemaKey("com.snowplowanalytics.snowplow", "campaign_attribution", "jsonschema", "1-0-0")

  /**
   * Creates a CampaignAttributionEnrichment instance from a JValue.
   * 
   * @param config The referer_parser enrichment JSON
   * @param schemaKey The SchemaKey provided for the enrichment
   *        Must be a supported SchemaKey for this enrichment   
   * @return a configured CampaignAttributionEnrichment instance
   */
  def parse(config: JValue, schemaKey: SchemaKey): ValidatedNelMessage[CampaignAttributionEnrichment] = {
    isParseable(config, schemaKey).flatMap( conf => {
      (for {
        medium    <- ScalazJson4sUtils.extract[List[String]](config, "parameters", "fields", "mktMedium")
        source    <- ScalazJson4sUtils.extract[List[String]](config, "parameters", "fields", "mktSource")
        term      <- ScalazJson4sUtils.extract[List[String]](config, "parameters", "fields", "mktTerm")
        content   <- ScalazJson4sUtils.extract[List[String]](config, "parameters", "fields", "mktContent")
        campaign  <- ScalazJson4sUtils.extract[List[String]](config, "parameters", "fields", "mktCampaign")

        enrich =  CampaignAttributionEnrichment(medium, source, term, content, campaign)
      } yield enrich).toValidationNel
    })
  }

}

/**
 * Class for a marketing campaign
 *
 * @param medium Campaign medium
 * @param source Campaign source
 * @param term Campaign term
 * @param content Campaign content
 * @param campaign Campaign name
 *
 */
case class MarketingCampaign(
  medium:   Option[String],
  source:   Option[String],
  term:     Option[String],
  content:  Option[String],
  campaign: Option[String]  
  )

/**
 * Config for a campaign_attribution enrichment
 *
 * @param mktMedium List of marketing medium parameters
 * @param mktSource List of marketing source parameters
 * @param mktTerm List of marketing term parameters
 * @param mktContent List of marketing content parameters
 * @param mktCampaign List of marketing campaign parameters
 */
case class CampaignAttributionEnrichment(
  mktMedium:   List[String],
  mktSource:   List[String],
  mktTerm:     List[String],
  mktContent:  List[String],
  mktCampaign: List[String]
  ) extends Enrichment {

  val version = new DefaultArtifactVersion("0.2.0")

  /**
   * Find the first string in parameterList which is a key of
   * sourceMap and return the value of that key in sourceMap.
   *
   * @param parameterList List of accepted campaign parameter
   *        names in order of decreasing precedence
   * @param sourceMap Map of key-value pairs in URI querystring
   * @return Option boxing the value of the campaign parameter
   */
  private def getFirstParameter(parameterList: List[String], sourceMap: SourceMap): Option[String] =
    parameterList.find(sourceMap.contains(_)).map(sourceMap(_))

  /**
   * Extract the marketing fields from a URL.
   *
   * @param uri The URI to extract
   *        marketing fields from
   * @param encoding The encoding of
   *        the URI being parsed
   * @return the MarketingCampaign
   *         or an error message,
   *         boxed in a Scalaz
   *         Validation
   */
  def extractMarketingFields(uri: URI, encoding: String): ValidationNel[String, MarketingCampaign] = {

    val parameters = try {
      URLEncodedUtils.parse(uri, encoding)
    } catch {
      case _ => return "Could not parse uri [%s]".format(uri).failNel[MarketingCampaign]
    }

    // Querystring map
    val sourceMap: SourceMap = parameters.map(p => (p.getName -> p.getValue)).toList.toMap

    val decodeString: TransformFunc = CU.decodeString(encoding, _, _)

    val medium = getFirstParameter(mktMedium, sourceMap)
    val source = getFirstParameter(mktSource, sourceMap)
    val term = getFirstParameter(mktTerm, sourceMap)
    val content = getFirstParameter(mktContent, sourceMap)
    val campaign = getFirstParameter(mktCampaign, sourceMap)

    MarketingCampaign(medium, source, term, content, campaign).success.toValidationNel
  }   

}
