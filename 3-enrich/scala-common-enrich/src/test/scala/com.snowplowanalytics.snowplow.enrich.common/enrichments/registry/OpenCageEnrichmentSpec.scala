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

class OpenCageEnrichmentUsingAPISpec extends Specification {
  def is: Fragments =
    s2"""
      This specification is to test the OpenCageEnrichment exercising the API
      OpenCache enrichment client is lazy    $e1
      """

  private def e1 = OpenCageEnrichment("KEY", 0, 1, 0) must not(throwA[IllegalArgumentException])
}
