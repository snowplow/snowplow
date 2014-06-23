/*
 * Copyright (c) 2012-2014 Snowplow Analytics Ltd. All rights reserved.
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
package enrichments

// Java
import java.net.URI

// Specs2, Scalaz-Specs2 & ScalaCheck
import org.specs2.{Specification, ScalaCheck}
import org.specs2.matcher.DataTables
import org.specs2.scalaz.ValidationMatchers
import org.scalacheck._
import org.scalacheck.Arbitrary._

// Scalaz
import scalaz._
import Scalaz._

// Scala MaxMind GeoIP
import com.snowplowanalytics.maxmind.geoip.{IpGeo, IpLocation}

class ExtractGeoLocationTest extends Specification with DataTables with ValidationMatchers with ScalaCheck { def is =

  "This is a specification to test the extractGeoLocation function"                                 ^
                                                                                                   p^
  "extractGeoLocation should not return failure for any valid or invalid IP address"                ! e1^
  "extractGeoLocation should correctly extract location data from IP addresses where possible"      ! e2^
                                                                                                    end

  //val dbFile = getClass.getResource("/maxmind/GeoLiteCity.dat").toURI.getPath
  //val ipGeo = IpGeo(dbFile, memCache = true, lruCache = 20000)

  val config = IpToGeoEnrichment(new URI("/not-used/"), "GeoLiteCity.dat", true)

  // Impossible to make extractIpLocation throw a validation error
  def e1 =
    check { (ipAddress: String) => config.extractGeoLocation(ipAddress) must beSuccessful }

  def e2 =
    "SPEC NAME"             || "IP ADDRESS"  | "EXPECTED LOCATION" |
    "blank IP address"      !! ""            ! None                |
    "null IP address"       !! null          ! None                |
    "invalid IP address #1" !! "localhost"   ! None                |
    "invalid IP address #2" !! "hello"       ! None                |
    "valid IP address"      !! "128.232.0.0" ! Some(IpLocation(    // Taken from scala-maxmind-geoip. See that test suite for other valid IP addresses
                                                 countryCode = "GB",
                                                 countryName = "United Kingdom",
                                                 region = Some("C3"),
                                                 city = Some("Cambridge"),
                                                 latitude = 52.199997F,
                                                 longitude = 0.11669922F,
                                                 postalCode = None,
                                                 dmaCode = None,
                                                 areaCode = None,
                                                 metroCode = None
                                               ))                  |> {
      (_, ipAddress, expected) =>
        config.extractGeoLocation(ipAddress) must beSuccessful(expected)
    }
}