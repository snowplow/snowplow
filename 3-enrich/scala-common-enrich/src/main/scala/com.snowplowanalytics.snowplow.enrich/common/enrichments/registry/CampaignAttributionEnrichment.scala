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

// Maven Artifact
import org.apache.maven.artifact.versioning.DefaultArtifactVersion

// Scalaz
import scalaz._
import Scalaz._

// json4s
import org.json4s.JValue

import iglu.client.{
  SchemaCriterion,
  SchemaKey
}

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

  val supportedSchema = SchemaCriterion("com.snowplowanalytics.snowplow", "campaign_attribution", "jsonschema", 1, 0)

  val DefaultNetworkMap = Map(
    "gclid" -> "Google",
    "msclkid" -> "Microsoft",
    "dclid" -> "DoubleClick"
  )

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

        customClickMap = ScalazJson4sUtils.extract[Map[String, String]](config, "parameters", "fields", "mktClickId").fold(
          // Assign empty Map on missing property for backwards compatibility with schema version 1-0-0
          e => Map(),
          s => s
        )

        enrich =  CampaignAttributionEnrichment(medium, source, term, content, campaign, (DefaultNetworkMap ++ customClickMap).toList)
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
 * @param clickId Click ID
 * @param network Advertising network
 */
case class MarketingCampaign(
  medium:   Option[String],
  source:   Option[String],
  term:     Option[String],
  content:  Option[String],
  campaign: Option[String],
  clickId:  Option[String],
  network : Option[String]
  )

/**
 * Config for a campaign_attribution enrichment
 *
 * @param mediumParameters List of marketing medium parameters
 * @param sourceParameters List of marketing source parameters
 * @param termParameters List of marketing term parameters
 * @param contentParameters List of marketing content parameters
 * @param campaignParameters List of marketing campaign parameters
 * @param mktClick: Map of click ID parameters to networks
 */
case class CampaignAttributionEnrichment(
  mediumParameters:   List[String],
  sourceParameters:   List[String],
  termParameters:     List[String],
  contentParameters:  List[String],
  campaignParameters: List[String],
  clickIdParameters:  List[(String, String)]
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
   * @param nvPairs The querystring to extract
   *        marketing fields from
   * @return the MarketingCampaign
   *         or an error message,
   *         boxed in a Scalaz
   *         Validation
   */
  def extractMarketingFields(nvPairs: SourceMap): ValidationNel[String, MarketingCampaign] = {
    val medium = getFirstParameter(mediumParameters, nvPairs)
    val source = getFirstParameter(sourceParameters, nvPairs)
    val term = getFirstParameter(termParameters, nvPairs)
    val content = getFirstParameter(contentParameters, nvPairs)
    val campaign = getFirstParameter(campaignParameters, nvPairs)

    val (clickId, network) = Unzip[Option].unzip(clickIdParameters.find(pair => nvPairs.contains(pair._1)).map(pair => (nvPairs(pair._1), pair._2)))

    MarketingCampaign(medium, source, term, content, campaign, clickId, network).success.toValidationNel
  }

}
