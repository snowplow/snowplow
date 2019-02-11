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
import com.snowplowanalytics.iglu.client.{SchemaCriterion, SchemaKey}
import com.snowplowanalytics.snowplow.enrich.common.ValidatedNelMessage
import com.snowplowanalytics.snowplow.enrich.common.utils.ScalazJson4sUtils
import com.opencagedata.geocoder.{OpenCageClient, OpenCageResponse, OpencageClientError, TimeoutError}
import com.twitter.util.SynchronizedLruMap
import org.json4s.JValue

import scala.concurrent.duration._
import scala.concurrent.Await
import scala.util.Try
import scala.util.control.NonFatal

// Java
import java.lang.{Float => JFloat}

// Scalaz
import scalaz._
import Scalaz._

// json4s
import org.json4s.{DefaultFormats, JObject, JString, JValue}
import org.json4s.Extraction
import org.json4s.JsonDSL._

/**
 * Companion object. Lets us create an OpenCageEnrichment instance from a JValue
 */
object OpenCageEnrichmentConfig extends ParseableEnrichment {
  implicit val defaultFormats: DefaultFormats.type = DefaultFormats

  override val supportedSchema: SchemaCriterion =
    SchemaCriterion("com.snowplowanalytics.snowplow.enrichments", "opencage_enrichment_config", "jsonschema", 1, 0)

  def parse(config: JValue, schemaKey: SchemaKey): ValidatedNelMessage[OpenCageEnrichment] =
    isParseable(config, schemaKey).flatMap { conf =>
      {
        (for {
          apiKey       <- ScalazJson4sUtils.extract[String](config, "parameters", "apiKey")
          cacheSize    <- ScalazJson4sUtils.extract[Int](config, "parameters", "cacheSize")
          geoPrecision <- ScalazJson4sUtils.extract[Int](config, "parameters", "geoPrecision")
          timeout      <- ScalazJson4sUtils.extract[Int](config, "parameters", "timeout")
        } yield OpenCageEnrichment(apiKey, cacheSize, geoPrecision, timeout)).toValidationNel
      }
    }
}

/**
 * Contains location enrichment based on geo coordinates and time
 *
 * @param apiKey OpenCage's API key
 */
case class OpenCageEnrichment(apiKey: String, cacheSize: Int, geoPrecision: Int, timeout: Int) extends Enrichment {
  private lazy val client = new OpenCageClient(apiKey)

  private val schemaUri = "iglu:com.opencagedata/geocoder/jsonschema/1-0-0"

  private implicit val formats = DefaultFormats

  protected val cache = OpencageCache(client, cacheSize, geoPrecision, timeout)

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
        getCachedOrRequest(lat, lon).flatMap { geocoding =>
          // TODO. Fix hardcoding variable
          JObject(List(("aux", JString("Hola")))).success
        }

      case _ => s"One of the required event fields missing. latitude $latitude, longitude: $longitude".fail
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

}

case class OpencageCache(
  client: OpenCageClient,
  cacheSize: Int, // Size of the LRU cache
  geoPrecision: Int, // nth part of one to round geocoordinates
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
   * Round coordinate by `geoPrecision`
   * Scale value to tenths to prevent values to be long like 1.333334
   *
   * @param coordinate latitude or longitude
   * @return rounded coordinate
   */
  def roundCoordinate(coordinate: Float): Float =
    BigDecimal
      .decimal(Math.round(coordinate * geoPrecision) / geoPrecision.toFloat)
      .setScale(1, BigDecimal.RoundingMode.HALF_UP)
      .toFloat
}
