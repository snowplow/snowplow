/*
 * Copyright (c) 2012-2013 SnowPlow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.hadoop.etl
package enrichments
package web

// Java
import java.net.URI

// Scala
import scala.collection.JavaConversions._
import scala.reflect.BeanProperty

// Scalaz
import scalaz._
import Scalaz._

// Apache URLEncodedUtils
import org.apache.http.NameValuePair
import org.apache.http.client.utils.URLEncodedUtils

// This project
import utils.{ConversionUtils => CU}
import utils.DataTransform._

/**
 * Holds enrichments related to marketing and campaign
 * attribution.
 */
object AttributionEnrichments {

  /**
   * Class for a marketing campaign. Any or
   * all of the five fields can be set.
   */
  class MarketingCampaign {
    @BeanProperty var medium: String = _
    @BeanProperty var source: String = _
    @BeanProperty var term: String = _
    @BeanProperty var content: String = _
    @BeanProperty var campaign: String = _
  }

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
  def extractMarketingFields(uri: URI, encoding: String): ValidationNEL[String, MarketingCampaign] = {

    val parameters = try {
      URLEncodedUtils.parse(uri, encoding)
    } catch {
      case _ => return "Could not parse uri [%s]".format(uri).failNel[MarketingCampaign]
    }

    val decodeString: TransformFunc = CU.decodeString(encoding, _, _)

    // We use a TransformMap which takes the format:
    // "source key" -> (transformFunction, field(s) to set)
    val transformMap: TransformMap =
      Map(("utm_medium"   , (decodeString, "medium")),
          ("utm_source"   , (decodeString, "source")),
          ("utm_term"     , (decodeString, "term")),
          ("utm_content"  , (decodeString, "term")),
          ("utm_campaign" , (decodeString, "campaign")))

    val sourceMap: SourceMap = parameters.map(p => (p.getName -> p.getValue)).toList.toMap

    val campaign = new MarketingCampaign
    val transform = campaign.transform(sourceMap, transformMap)
    transform.flatMap(s => campaign.success)
  }
}