/**
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
package com.snowplowanalytics.snowplow.enrich.common.enrichments.registry

import java.lang.{Float => JFloat}

import com.snowplowanalytics.iglu.client.SchemaKey
import io.circe.generic.auto._
import io.circe.literal._
import org.joda.time.DateTime
import org.specs2.Specification

object WeatherEnrichmentSpec {
  val OwmApiKey = "OWM_KEY"
}

class WeatherEnrichmentSpec extends Specification {
  import WeatherEnrichmentSpec._
  def is =
    skipAllIf(sys.env.get(OwmApiKey).isEmpty) ^ // Actually only e4 and e6 need to be skipped
      s2"""
  This is a specification to test the WeatherEnrichment
  Fail event for null time          $e1
  Fail event for invalid key        $e4
  Weather enrichment client is lazy $e2
  Extract weather stamp             $e3
  Extract humidity                  $e5
  Extract configuration             $e6
  Check time stamp transformation   $e7
  """

  lazy val validAppKey = sys.env
    .get(OwmApiKey)
    .getOrElse(throw new IllegalStateException(
      s"No $OwmApiKey environment variable found, test should have been skipped"))

  object invalidEvent {
    var lat: JFloat = 70.98224f
    var lon: JFloat = 70.98224f
    var time: DateTime = null
  }

  object validEvent {
    var lat: JFloat    = 20.713052f
    var lon: JFloat    = 70.98224f
    var time: DateTime = new DateTime("2019-04-30T23:56:01.003+00:00")
  }

  def e1 = {
    val enr = WeatherEnrichment("KEY", 5200, 1, "history.openweathermap.org", 10)
    val stamp = enr.getWeatherContext(
      Option(invalidEvent.lat),
      Option(invalidEvent.lon),
      Option(invalidEvent.time))
    stamp must beLeft.like { case e => e must contain("tstamp: None") }
  }

  def e2 =
    WeatherEnrichment("KEY", 0, 1, "history.openweathermap.org", 5) must not(
      throwA[IllegalArgumentException])

  def e3 = {
    val enr = WeatherEnrichment(validAppKey, 5200, 1, "history.openweathermap.org", 10)
    val stamp =
      enr.getWeatherContext(Option(validEvent.lat), Option(validEvent.lon), Option(validEvent.time))
    stamp must beRight
  }

  def e4 = {
    val enr = WeatherEnrichment("KEY", 5200, 1, "history.openweathermap.org", 10)
    val stamp =
      enr.getWeatherContext(Option(validEvent.lat), Option(validEvent.lon), Option(validEvent.time))
    stamp must beLeft.like { case e => e must contain("AuthorizationError") }
  }

  def e5 = {
    val enr = WeatherEnrichment(validAppKey, 5200, 1, "history.openweathermap.org", 15)
    val stamp =
      enr.getWeatherContext(Option(validEvent.lat), Option(validEvent.lon), Option(validEvent.time))
    stamp must beRight.like {
      case weather =>
        val temp = weather.hcursor.get[Double]("humidity")
        temp must beRight(87.0d)
    }
  }

  def e6 = {
    val configJson = json"""
      {
          "enabled": true,
          "vendor": "com.snowplowanalytics.snowplow.enrichments",
          "name": "weather_enrichment_config",
          "parameters": {
              "apiKey": "{{KEY}}",
              "cacheSize": 5100,
              "geoPrecision": 1,
              "apiHost": "history.openweathermap.org",
              "timeout": 5
          }
      }
    """
    val config = WeatherEnrichmentConfig.parse(
      configJson,
      SchemaKey(
        "com.snowplowanalytics.snowplow.enrichments",
        "weather_enrichment_config",
        "jsonschema",
        "1-0-0"))
    config.toEither must beRight(
      WeatherEnrichment(
        apiKey = "{{KEY}}",
        geoPrecision = 1,
        cacheSize = 5100,
        apiHost = "history.openweathermap.org",
        timeout = 5))
  }

  def e7 = {
    val enr = WeatherEnrichment(validAppKey, 2, 1, "history.openweathermap.org", 15)
    val stamp =
      enr.getWeatherContext(Option(validEvent.lat), Option(validEvent.lon), Option(validEvent.time))
    stamp must beRight.like { // successful request
      case weather =>
        weather.hcursor.get[TransformedWeather]("data") must beRight.like {
          case w => w.dt must equalTo("2019-05-01T00:00:00.000Z")
        }
    }
  }

}
