/**Copyright (c) 2012-2019 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.enrich.common.enrichments.registry
import org.json4s.{DefaultFormats, JObject}
import org.specs2.Specification
import org.specs2.specification.Fragments

// Java
import java.lang.{Float => JFloat}

class OpenCageEnrichmentSpec extends Specification {

  implicit val formats = DefaultFormats

  val OpenCageApiKey = "OPENCAGE_KEY"

  def is: Fragments =
    skipAllIf(sys.env.get(OpenCageApiKey).isEmpty) ^
      s2"""
      This specification is to test the OpenCageEnrichment
      Fail event for invalid key         $e1
      Extract location                   $e2
      """

  object ValidPosition {
    val lat: JFloat = 41.390205f
    val lon: JFloat = 2.154007f
  }

  val reasonableTimeout = 5

  lazy val validApiKey: String = sys.env
    .getOrElse(
      OpenCageApiKey,
      throw new IllegalStateException(s"No $OpenCageApiKey environment variable found, test should have been skipped"))

  private def e1 = {
    val enrichment = OpenCageEnrichment("KEY", 1, 1, reasonableTimeout)
    val obtained   = enrichment.getGeoCodingContext(Option(ValidPosition.lat), Option(ValidPosition.lon))
    obtained.toEither must beLeft.like { case e => e must contain("invalid API key") }
  }

  private def e2 = {
    val enrichment = OpenCageEnrichment(validApiKey, 1, 1, reasonableTimeout)
    val context    = enrichment.getGeoCodingContext(Option(ValidPosition.lat), Option(ValidPosition.lon))
    context.toEither must beRight.like {
      case location: JObject => {
        val city = (location \ "data" \ "formattedAddress").extract[String]
        city must contain("Barcelona, Spain")
      }
    }
  }

}

class OpenCageCacheSpec extends Specification {
  def is: Fragments =
    s2"""
        This specification is to test the OpenCageCage
        Round floats to meaningful precisions $e1
      """

  private def e1 =
    OpenCageCache.roundCoordinate(41.1234567f, 5) === 41.12345f

}

class OpenCageEnrichmentUsingAPISpec extends Specification {
  def is: Fragments =
    s2"""
      This specification is to test the OpenCageEnrichment exercising the API
      OpenCache enrichment client is lazy    $e1
      """

  private def e1 = OpenCageEnrichment("KEY", 0, 1, 0) must not(throwA[IllegalArgumentException])
}
