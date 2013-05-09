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
package com.snowplowanalytics.snowplow.enrich.hadoop
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

// Utils
import org.apache.http.NameValuePair
import org.apache.http.client.utils.URLEncodedUtils
import org.apache.commons.lang3.builder.ToStringBuilder
import org.apache.commons.lang3.builder.HashCodeBuilder

// Snowplow referer-parser
import com.snowplowanalytics.refererparser.scala.{Parser => RefererParser}
import com.snowplowanalytics.refererparser.scala.Referer

// This project
import utils.{ConversionUtils => CU}
import utils.MapTransformer
import utils.MapTransformer._

/**
 * Holds enrichments related to marketing and campaign
 * attribution.
 */
object AttributionEnrichments {

  /**
   * Class for a marketing campaign. Any or
   * all of the five fields can be set.
   */
  // TODO: change this to a (much simpler) case class as soon
  // as MapTransformer supports case classes
  class MarketingCampaign {
    @BeanProperty var source: String = _
    @BeanProperty var medium: String = _
    @BeanProperty var term: String = _
    @BeanProperty var content: String = _
    @BeanProperty var campaign: String = _

    override def equals(other: Any): Boolean = other match {
      case that: MarketingCampaign =>
        (that canEqual this) &&
        source == that.source &&
        medium == that.medium &&
        term == that.term &&
        content == that.content &&
        campaign == that.campaign
      case _ => false
    }
    def canEqual(other: Any): Boolean = other.isInstanceOf[MarketingCampaign]
    
    // No reflection for perf reasons.
    override def hashCode: Int = new HashCodeBuilder()
      .append(source)
      .append(medium)
      .append(term)
      .append(content)
      .append(campaign)
      .toHashCode()
    override def toString: String = new ToStringBuilder(this)
      .append("source", source)
      .append("medium", medium)
      .append("term", term)
      .append("content", content)
      .append("campaign", campaign)
      .toString()
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
  def extractMarketingFields(uri: URI, encoding: String): ValidationNel[String, MarketingCampaign] = {

    val parameters = try {
      URLEncodedUtils.parse(uri, encoding)
    } catch {
      case _ => return "Could not parse uri [%s]".format(uri).failNel[MarketingCampaign]
    }

    val decodeString: TransformFunc = CU.decodeString(encoding, _, _)

    // We use a TransformMap which takes the format:
    // "source key" -> (transformFunction, field(s) to set)
    val transformMap: TransformMap =
      Map(("utm_source"   , (decodeString, "source")),
          ("utm_medium"   , (decodeString, "medium")),
          ("utm_term"     , (decodeString, "term")),
          ("utm_content"  , (decodeString, "content")),
          ("utm_campaign" , (decodeString, "campaign")))

    val sourceMap: SourceMap = parameters.map(p => (p.getName -> p.getValue)).toList.toMap

    MapTransformer.generate[MarketingCampaign](sourceMap, transformMap)
  }

  /**
   * A Scalaz Lens to update the term within
   * a Referer object.
   */
  private val termLens: Lens[Referer, MaybeString] = Lens.lensu((r, newTerm) => r.copy(term = newTerm), _.term)

  /**
   * Extract details about the referer (sic).
   *
   * Uses the referer-parser library. 
   *
   * @param uri The referer URI to extract
   *            referer details from
   * @param pageHost The host of the current
   *                 page (used to determine
   *                 if this is an internal
   *                 referer)
   * @return a Tuple3 containing referer medium,
   *         source and term, all Strings
   */
  def extractRefererDetails(uri: URI, pageHost: String): Option[Referer] = {
    for {
      r <- RefererParser.parse(uri, pageHost)
      t = r.term.flatMap(t => CU.fixTabsNewlines(t))
    } yield termLens.set(r, t)
  }
}