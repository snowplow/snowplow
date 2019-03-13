/*
 * Copyright (c) 2012-2019 Snowplow Analytics Ltd. All rights reserved.
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

import java.lang.{Float => JFloat}

import scala.util.control.NonFatal

import com.snowplowanalytics.iglu.client.{SchemaCriterion, SchemaKey}
import com.snowplowanalytics.weather.providers.openweather.OwmCacheClient
import com.snowplowanalytics.weather.providers.openweather.Responses._
import io.circe._
import io.circe.generic.auto._
import io.circe.syntax._
import org.joda.time.{DateTime, DateTimeZone}
import scalaz._
import Scalaz._

import utils.ScalazCirceUtils

/** Companion object. Lets us create an WeatherEnrichment instance from a Json */
object WeatherEnrichmentConfig extends ParseableEnrichment {
  val supportedSchema =
    SchemaCriterion(
      "com.snowplowanalytics.snowplow.enrichments",
      "weather_enrichment_config",
      "jsonschema",
      1,
      0)

  def parse(config: Json, schemaKey: SchemaKey): ValidatedNelMessage[WeatherEnrichment] =
    isParseable(config, schemaKey).flatMap { conf =>
      (for {
        apiKey <- ScalazCirceUtils.extract[String](config, "parameters", "apiKey")
        cacheSize <- ScalazCirceUtils.extract[Int](config, "parameters", "cacheSize")
        geoPrecision <- ScalazCirceUtils.extract[Int](config, "parameters", "geoPrecision")
        apiHost <- ScalazCirceUtils.extract[String](config, "parameters", "apiHost")
        timeout <- ScalazCirceUtils.extract[Int](config, "parameters", "timeout")
        enrich = WeatherEnrichment(apiKey, cacheSize, geoPrecision, apiHost, timeout)
      } yield enrich).toValidationNel
    }
}

/**
 * Contains weather enrichments based on geo coordinates and time
 * @param apiKey weather provider API KEY
 * @param cacheSize amount of days with prefetched weather
 * @param geoPrecision rounder for geo lat/long floating, which allows to use more spatially
 * precise weather stamps
 * @param apiHost address of weather provider's API host
 * @param timeout timeout in seconds to fetch weather from server
 */
case class WeatherEnrichment(
  apiKey: String,
  cacheSize: Int,
  geoPrecision: Int,
  apiHost: String,
  timeout: Int
) extends Enrichment {
  private lazy val client = OwmCacheClient(apiKey, cacheSize, geoPrecision, apiHost, timeout)

  private val schemaUri = "iglu:org.openweathermap/weather/jsonschema/1-0-0"

  /**
   * Get weather context as JSON for specific event. Any non-fatal error will return failure and
   * thus whole event will be filtered out in future
   * @param latitude enriched event optional latitude (probably null)
   * @param longitude enriched event optional longitude (probably null)
   * @param time enriched event optional time (probably null)
   * @return weather stamp as self-describing JSON object
   */
  // It accepts Java Float (JFloat) instead of Scala's because it will throw NullPointerException
  // on conversion step if `EnrichedEvent` has nulls as geo_latitude or geo_longitude
  def getWeatherContext(
    latitude: Option[JFloat],
    longitude: Option[JFloat],
    time: Option[DateTime]
  ): Validation[String, Json] =
    try {
      getWeather(latitude, longitude, time).map(addSchema)
    } catch {
      case NonFatal(exc) => exc.getMessage.fail
    }

  /**
   * Get weather stamp as JSON received from OpenWeatherMap and extracted with Scala Weather
   * @param latitude enriched event optional latitude
   * @param longitude enriched event optional longitude
   * @param time enriched event optional time
   * @return weather stamp as JSON object
   */
  private def getWeather(
    latitude: Option[JFloat],
    longitude: Option[JFloat],
    time: Option[DateTime]
  ): Validation[String, Json] =
    (latitude, longitude, time) match {
      case (Some(lat), Some(lon), Some(t)) =>
        getCachedOrRequest(lat, lon, (t.getMillis / 1000).toInt).flatMap { weatherStamp =>
          val transformedWeather = transformWeather(weatherStamp)
          transformedWeather.asJson.success
        }
      case _ =>
        ("One of required event fields missing. latitude: " +
          s"$latitude, longitude: $longitude, tstamp: $time").fail
    }

  /**
   * Return weather, convert disjunction to validation and stringify error
   * @param latitude event latitude
   * @param longitude event longitude
   * @param timestamp event timestamp
   * @return optional weather stamp
   */
  private def getCachedOrRequest(
    latitude: Float,
    longitude: Float,
    timestamp: Int): Validation[String, Weather] =
    client.getCachedOrRequest(latitude, longitude, timestamp) match {
      case Right(w) => w.success
      case Left(e) => e.toString.failure
    }

  /**
   * Add Iglu URI to JSON Object
   * @param context weather context as JSON Object
   * @return JSON Object wrapped as Self-describing JSON
   */
  private def addSchema(context: Json): Json =
    Json.obj(
      "schema" := schemaUri,
      "data" := context
    )

  /**
   * Apply all necessary transformations (currently only dt(epoch -> db timestamp)
   * from `weather.providers.openweather.Responses.Weather` to `TransformedWeather`
   * for further JSON decomposition
   *
   * @param origin original OpenWeatherMap Weather stamp
   * @return tranfsormed weather
   */
  private[enrichments] def transformWeather(origin: Weather): TransformedWeather = {
    val time = new DateTime(origin.dt.toLong * 1000, DateTimeZone.UTC).toString
    TransformedWeather(
      origin.main,
      origin.wind,
      origin.clouds,
      origin.rain,
      origin.snow,
      origin.weather,
      time)
  }
}

/**
 * Copy of `com.snowplowanalytics.weather.providers.openweather.Responses.Weather` intended to
 * execute typesafe (as opposed to JSON) transformation
 */
private[enrichments] final case class TransformedWeather(
  main: MainInfo,
  wind: Wind,
  clouds: Clouds,
  rain: Option[Rain],
  snow: Option[Snow],
  weather: List[WeatherCondition],
  dt: String
)
