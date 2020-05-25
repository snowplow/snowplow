/*
 * Copyright (c) 2014-2020 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.enrich.common
package enrichments.registry

import cats.data.{NonEmptyList, ValidatedNel}
import cats.implicits._
import com.snowplowanalytics.iglu.core.{SchemaCriterion, SchemaKey}
import io.circe._

import utils.MapTransformer.SourceMap
import utils.CirceUtils

/** Companion object. Lets us create a CampaignAttributionEnrichment from a Json */
object CampaignAttributionEnrichment extends ParseableEnrichment {
  override val supportedSchema =
    SchemaCriterion("com.snowplowanalytics.snowplow", "campaign_attribution", "jsonschema", 1, 0)

  val DefaultNetworkMap = Map(
    "gclid" -> "Google",
    "msclkid" -> "Microsoft",
    "dclid" -> "DoubleClick"
  )

  /**
   * Creates a CampaignAttributionConf instance from a Json.
   * @param c The referer_parser enrichment JSON
   * @param schemaKey provided for the enrichment, must be supported by this enrichment
   * @return a CampaignAttributionEnrichment configuration
   */
  override def parse(
    c: Json,
    schemaKey: SchemaKey,
    localMode: Boolean = false
  ): ValidatedNel[String, CampaignAttributionConf] =
    isParseable(c, schemaKey)
      .leftMap(e => NonEmptyList.one(e))
      .flatMap { _ =>
        (
          CirceUtils.extract[List[String]](c, "parameters", "fields", "mktMedium").toValidatedNel,
          CirceUtils.extract[List[String]](c, "parameters", "fields", "mktSource").toValidatedNel,
          CirceUtils.extract[List[String]](c, "parameters", "fields", "mktTerm").toValidatedNel,
          CirceUtils.extract[List[String]](c, "parameters", "fields", "mktContent").toValidatedNel,
          CirceUtils.extract[List[String]](c, "parameters", "fields", "mktCampaign").toValidatedNel
        ).mapN { (medium, source, term, content, campaign) =>
          // Assign empty Map on missing property for backwards compatibility with schema version 1-0-0
          val customClickMap = CirceUtils
            .extract[Map[String, String]](c, "parameters", "fields", "mktClickId")
            .fold(_ => Map(), s => s)
          CampaignAttributionConf(
            medium,
            source,
            term,
            content,
            campaign,
            (DefaultNetworkMap ++ customClickMap).toList
          )
        }.toEither
      }
      .toValidated
}

/**
 * Class for a marketing campaign
 * @param medium Campaign medium
 * @param source Campaign source
 * @param term Campaign term
 * @param content Campaign content
 * @param campaign Campaign name
 * @param clickId Click ID
 * @param network Advertising network
 */
final case class MarketingCampaign(
  medium: Option[String],
  source: Option[String],
  term: Option[String],
  content: Option[String],
  campaign: Option[String],
  clickId: Option[String],
  network: Option[String]
)

/**
 * Config for a campaign_attribution enrichment
 * @param mediumParameters List of marketing medium parameters
 * @param sourceParameters List of marketing source parameters
 * @param termParameters List of marketing term parameters
 * @param contentParameters List of marketing content parameters
 * @param campaignParameters List of marketing campaign parameters
 * @param mktClick Map of click ID parameters to networks
 */
final case class CampaignAttributionEnrichment(
  mediumParameters: List[String],
  sourceParameters: List[String],
  termParameters: List[String],
  contentParameters: List[String],
  campaignParameters: List[String],
  clickIdParameters: List[(String, String)]
) extends Enrichment {

  /**
   * Find the first string in parameterList which is a key of sourceMap and return the value of that
   * key in sourceMap.
   * @param parameterList List of accepted parameter names in order of decreasing precedence
   * @param sourceMap Map of key-value pairs in URI querystring
   * @return Option boxing the value of the campaign parameter
   */
  private def getFirstParameter(parameterList: List[String], sourceMap: SourceMap): Option[String] =
    parameterList.find(sourceMap.contains(_)).map(sourceMap(_))

  /**
   * Extract the marketing fields from a URL.
   * @param qsList The querystring to extract marketing fields from
   * @return the MarketingCampaign or an error message, boxed in a Scalaz Validation
   */
  def extractMarketingFields(qsList: QueryStringParameters): MarketingCampaign = {
    val qsMap = qsList.toMap.map { case (k, v) => (k, v.getOrElse("")) }
    val medium = getFirstParameter(mediumParameters, qsMap)
    val source = getFirstParameter(sourceParameters, qsMap)
    val term = getFirstParameter(termParameters, qsMap)
    val content = getFirstParameter(contentParameters, qsMap)
    val campaign = getFirstParameter(campaignParameters, qsMap)

    val (clickId, network) =
      clickIdParameters
        .find(pair => qsMap.contains(pair._1))
        .map(pair => (qsMap(pair._1), pair._2))
        .separate

    MarketingCampaign(medium, source, term, content, campaign, clickId, network)
  }

}
