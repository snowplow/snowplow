/*
 * Copyright (c) 2012-2018 Snowplow Analytics Ltd. All rights reserved.
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
import com.opencagedata.geocoder._
import com.snowplowanalytics.iglu.client.{SchemaCriterion, SchemaKey}
import com.snowplowanalytics.snowplow.enrich.common.ValidatedNelMessage
import com.snowplowanalytics.snowplow.enrich.common.utils.ScalazJson4sUtils
import com.twitter.util.SynchronizedLruMap

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.Try
import scala.util.control.NonFatal

// Java
import java.lang.{Float => JFloat}

// Scalaz
import scalaz.Scalaz._
import scalaz._

// json4s
import org.json4s.JsonDSL._
import org.json4s.{DefaultFormats, Extraction, JObject, JValue}

/**
 * Companion object. Lets us create an OpenCageEnrichment instance from a JValue
 */
object OpenCageEnrichmentConfig extends ParseableEnrichment {
  implicit val defaultFormats: DefaultFormats.type = DefaultFormats

  override val supportedSchema: SchemaCriterion =
    SchemaCriterion("com.snowplowanalytics.snowplow.enrichments", "opencage_enrichment_config", "jsonschema", 1, 0)

  def parse(config: JValue, schemaKey: SchemaKey): ValidatedNelMessage[OpenCageEnrichment] =
    isParseable(config, schemaKey).flatMap { _ =>
      {
        (for {
          apiKey    <- ScalazJson4sUtils.extract[String](config, "parameters", "apiKey")
          cacheSize <- ScalazJson4sUtils.extract[Int](config, "parameters", "cacheSize")
          scale     <- ScalazJson4sUtils.extract[Int](config, "parameters", "scale")
          timeout   <- ScalazJson4sUtils.extract[Int](config, "parameters", "timeout")
        } yield OpenCageEnrichment(apiKey, cacheSize, scale, timeout)).toValidationNel
      }
    }
}

/**
 * Contains location enrichment based on geo coordinates and time
 *
 * @param apiKey OpenCage's API key
 * @param cacheSize amount of locations with prefetched geolocation info
 * @param scale Number of decimals to keep from coordinates. Above 6 is useless
 * @param timeout Number of seconds to wait for OpenCage API to respond
 */
case class OpenCageEnrichment(apiKey: String, cacheSize: Int, scale: Int, timeout: Int) extends Enrichment {

  type Geocode = parts.Result

  private lazy val client = new OpenCageClient(apiKey)

  private val schemaUri = "iglu:com.opencagedata/geocoder/jsonschema/1-0-0"

  private implicit val formats: DefaultFormats.type = DefaultFormats

  protected val cache = OpencageCache(client, cacheSize, scale, timeout)

  /**
   * Get textual description of a location for a specific event
   * Any non-fatal error will return failure and thus whole event will be
   * filtered out in future
   *
   * @param latitude enriched event optional latitude (probably null)
   * @param longitude enriched event optional longitude (probably null)
   * @return weather stamp as self-describing JSON object
   */
  // It accepts Java Float (JFloat) instead of Scala's because it will throw NullPointerException
  // on conversion step if `EnrichedEvent` has nulls as geo_latitude or geo_longitude
  def getGeoCodingContext(latitude: Option[JFloat], longitude: Option[JFloat]): Validation[String, JObject] =
    try {
      getGeocoding(latitude, longitude).map(addSchema)
    } catch {
      case NonFatal(ex) => ex.toString.fail
    }

  /**
   * Get geocoding of a latitude, longitude pair
   */
  private def getGeocoding(latitude: Option[JFloat], longitude: Option[JFloat]): Validation[String, JObject] =
    (latitude, longitude) match {
      case (Some(lat), Some(lon)) =>
        for {
          request <- getCachedOrRequest(lat, lon)
          geocode <- obtainGeocode(request)
          obj     <- geocode2JValue(geocode)
        } yield obj

      case _ => s"One of the required event fields missing. latitude $latitude, longitude: $longitude".fail
    }

  private def geocode2JValue(location: Geocode): Validation[String, JObject] =
    Extraction.decompose(location) match {
      case obj: JObject => obj.success
      case _            => s"Couldn't transform location object $location into JSON".fail // Shouldn't ever happen
    }

  /**
   * Return geocoding
   */
  private def getCachedOrRequest(latitude: Float, longitude: Float): Validation[String, OpenCageResponse] =
    cache.getCachedOrRequest(latitude, longitude) match {
      case Right(location) => location.success
      case Left(error)     => error.getMessage.failure
    }

  /**
   * Add Iglu URI to JSON Object
   *
   * @param context weather context as JSON Object
   * @return JSON Object wrapped as Self-describing JSON
   */
  private def addSchema(context: JObject): JObject =
    ("schema", schemaUri) ~ (("data", context))

  /**
   * From `com.opencagedata.geocoder.OpenCageResponse` obtain the most relevant result
   * for further JSON Object decomposition
   *
   * @param origin original OpenCageResponse object
   * @return Most relevant Geocode location
   */
  private[enrichments] def obtainGeocode(origin: OpenCageResponse): Validation[String, Geocode] =
    origin.results.headOption
      .toSuccess("Empty list of results although the call went OK")

}

private[enrichments] case class TransformedLocation(address: String)

case class OpencageCache(
  client: OpenCageClient,
  cacheSize: Int, // Size of the LRU cache
  scale: Int, // numbers of decimals to keep from latitudes and longitudes
  timeout: Int // Number of seconds we wait for a response from Opencage
) {

  private val secondsTimeout = timeout.seconds

  case class CacheKey(latitude: Float, longitude: Float)

  type Cache = SynchronizedLruMap[CacheKey, Either[OpencageClientError, OpenCageResponse]]
  protected val cache: Cache = new Cache(cacheSize)

  def getCachedOrRequest(latitude: Float, longitude: Float): Either[OpencageClientError, OpenCageResponse] = {
    val key = this.coordToCacheKey(latitude, longitude)
    cache.get(key) match {
      case Some(Right(cached))         => Right(cached)
      case Some(Left(TimeoutError(_))) => getAndCache(latitude, longitude)
      case Some(Left(error))           => Left(error)
      case None                        => getAndCache(latitude, longitude)
    }
  }

  private def getAndCache(latitude: Float, longitude: Float): Either[OpencageClientError, OpenCageResponse] = {
    val response = Try(Await.result(client.reverseGeocode(latitude, longitude), secondsTimeout))
    response match {
      case util.Success(value) => Right(value)
      case util.Failure(ex)    => Left(ex.asInstanceOf[OpencageClientError])
    }
  }

  private def coordToCacheKey(latitude: Float, longitude: Float) =
    CacheKey(roundCoordinate(latitude), roundCoordinate(longitude))

  /**
   * Truncate coordinates at position scale
   * Scale value to tenths to prevent values to be long like 1.333334
   *
   * @param coordinate latitude or longitude
   * @return rounded coordinate
   */
  def roundCoordinate(coordinate: Float): Float =
    BigDecimal
      .decimal(coordinate)
      .setScale(this.scale, BigDecimal.RoundingMode.DOWN)
      .toFloat
}
