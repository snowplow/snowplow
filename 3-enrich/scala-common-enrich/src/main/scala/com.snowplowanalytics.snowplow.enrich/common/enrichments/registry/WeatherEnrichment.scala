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
import java.time.{Instant, ZoneOffset, ZonedDateTime}
import java.util.concurrent.TimeUnit

import scala.concurrent.duration.FiniteDuration

import cats.Monad
import cats.data.{EitherT, NonEmptyList, ValidatedNel}
import cats.implicits._
import com.snowplowanalytics.iglu.core.{SchemaCriterion, SchemaKey}
import com.snowplowanalytics.weather.providers.openweather._
import com.snowplowanalytics.weather.providers.openweather.responses._
import io.circe._
import io.circe.generic.auto._
import io.circe.syntax._
import org.joda.time.{DateTime, DateTimeZone}

import outputs._
import utils.CirceUtils

/** Companion object. Lets us create an WeatherEnrichment instance from a Json */
object WeatherEnrichment extends ParseableEnrichment {
  override val supportedSchema =
    SchemaCriterion(
      "com.snowplowanalytics.snowplow.enrichments",
      "weather_enrichment_config",
      "jsonschema",
      1,
      0
    )

  override def parse(
    c: Json,
    schemaKey: SchemaKey,
    localMode: Boolean = false
  ): ValidatedNel[String, WeatherConf] =
    isParseable(c, schemaKey)
      .leftMap(e => NonEmptyList.one(e))
      .flatMap { _ =>
        (
          CirceUtils.extract[String](c, "parameters", "apiHost").toValidatedNel,
          CirceUtils.extract[String](c, "parameters", "apiKey").toValidatedNel,
          CirceUtils.extract[Int](c, "parameters", "timeout").toValidatedNel,
          CirceUtils.extract[Int](c, "parameters", "cacheSize").toValidatedNel,
          CirceUtils.extract[Int](c, "parameters", "geoPrecision").toValidatedNel
        ).mapN { WeatherConf(schemaKey, _, _, _, _, _) }.toEither
      }
      .toValidated

  /**
   * Creates a WeatherEnrichment from a WeatherConf
   * @param conf Configuration for the weather enrichment
   * @return a weather enrichment
   */
  def apply[F[_]: Monad: CreateOWM](conf: WeatherConf): EitherT[F, String, WeatherEnrichment[F]] =
    EitherT(
      CreateOWM[F]
        .create(
          conf.apiHost,
          conf.apiKey,
          FiniteDuration(conf.timeout.toLong, TimeUnit.SECONDS),
          true,
          conf.cacheSize,
          conf.geoPrecision
        )
    ).leftMap(_.message)
      .map(c => WeatherEnrichment(conf.schemaKey, c))
}

/**
 * Contains weather enrichments based on geo coordinates and time
 * @param client OWM client to get the weather from
 */
final case class WeatherEnrichment[F[_]: Monad](schemaKey: SchemaKey, client: OWMCacheClient[F])
    extends Enrichment {
  private val schemaUri = "iglu:org.openweathermap/weather/jsonschema/1-0-0"
  private val enrichmentInfo =
    EnrichmentInformation(schemaKey, "weather").some

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
  ): F[Either[NonEmptyList[EnrichmentStageIssue], Json]] =
    (for {
      weather <- getWeather(latitude, longitude, time)
      schemaed = addSchema(weather)
    } yield schemaed).value

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
  ): EitherT[F, NonEmptyList[EnrichmentStageIssue], Json] =
    (latitude, longitude, time) match {
      case (Some(lat), Some(lon), Some(t)) =>
        val ts = ZonedDateTime.ofInstant(Instant.ofEpochMilli(t.getMillis()), ZoneOffset.UTC)
        for {
          weather <- EitherT(client.cachingHistoryByCoords(lat, lon, ts))
            .leftMap { e =>
              val f =
                EnrichmentFailure(enrichmentInfo, SimpleEnrichmentFailureMessage(e.getMessage))
              NonEmptyList.one(f)
            }
            .leftWiden[NonEmptyList[EnrichmentStageIssue]]
          transformed = transformWeather(weather)
        } yield transformed.asJson
      case (a, b, c) =>
        val failures = List((a, "geo_latitude"), (b, "geo_longitude"), (c, "derived_tstamp"))
          .collect {
            case (None, n) =>
              EnrichmentFailure(
                enrichmentInfo,
                InputDataEnrichmentFailureMessage(n, none, "missing")
              )
          }
        EitherT.leftT(
          NonEmptyList
            .fromList(failures)
            .getOrElse(
              NonEmptyList.one(
                EnrichmentFailure(
                  enrichmentInfo,
                  SimpleEnrichmentFailureMessage("could not construct failures")
                )
              )
            )
        )
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
  private[enrichments] def transformWeather(origin: Weather): TransformedWeather =
    TransformedWeather(
      origin.main,
      origin.wind,
      origin.clouds,
      origin.rain,
      origin.snow,
      origin.weather,
      new DateTime(origin.dt.toLong * 1000, DateTimeZone.UTC).toString()
    )
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
