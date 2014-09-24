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
package registry

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
import com.snowplowanalytics.maxmind.iplookups.{
  IpLookups,
  IpLocation
}

class IpLookupsEnrichmentSpec extends Specification with DataTables with ValidationMatchers with ScalaCheck { def is =

  "This is a specification to test the IpLookupsEnrichment"                                           ^
                                                                                                     p^
  "extractIpInformation should not return failure for any valid or invalid IP address"                ! e1^
  "extractIpInformation should correctly extract location data from IP addresses where possible"      ! e2^
  "extractIpInformation should correctly extract ISP data from IP addresses where possible"           ! e3^
  "an IpLookupsEnrichment instance should expose no database files to cache in local mode"            ! e4^
  "an IpLookupsEnrichment instance should expose a list of database files to cache in non-local mode" ! e5^
                                                                                                      end
  // When testing, localMode is set to true, so the URIs are ignored and the databases are loaded from test/resources
  val config = IpLookupsEnrichment(Some(("geo", new URI("/ignored-in-local-mode/"), "GeoIPCity.dat")), Some(("isp", new URI("/ignored-in-local-mode/"), "GeoIPISP.dat")), None, None, None, true)

  // Impossible to make extractIpInformation throw a validation error
  def e1 =
    check { (ipAddress: String) => config.extractIpInformation(ipAddress) must beSuccessful }

  def e2 =
    "SPEC NAME"             || "IP ADDRESS"    | "EXPECTED LOCATION" |
    "blank IP address"      !! ""              ! None                |
    "null IP address"       !! null            ! None                |
    "invalid IP address #1" !! "localhost"     ! None                |
    "invalid IP address #2" !! "hello"         ! None                |
    "valid IP address"      !! "70.46.123.145" ! Some(IpLocation(    // Taken from scala-maxmind-geoip. See that test suite for other valid IP addresses
                                                 countryCode = "US",
                                                 countryName = "United States",
                                                 region = Some("FL"),
                                                 city = Some("Delray Beach"),
                                                 latitude = 26.461502F,
                                                 longitude = -80.0728F,
                                                 timezone = Some("America/New_York"),
                                                 postalCode = None,
                                                 dmaCode = Some(548),
                                                 areaCode = Some(561),
                                                 metroCode = Some(548),
                                                 regionName = Some("Florida")
                                               ))                  |> {
      (_, ipAddress, expected) =>
        config.extractIpInformation(ipAddress).map(_._1) must beSuccessful(expected)
    }

  def e3 = config.extractIpInformation("70.46.123.145").map(_._2) must beSuccessful(Some("FDN Communications"))

  def e4 = config.dbsToCache must_== Nil

  val configRemote = IpLookupsEnrichment(Some(("geo", new URI("http://public-website.com/files/GeoLiteCity.dat"), "GeoLiteCity.dat")), Some(("isp", new URI("s3://private-bucket/files/GeoIPISP.dat"), "GeoIPISP.dat")), None, None, None, false)

  def e5 = configRemote.dbsToCache must_== List(
    (new URI("http://public-website.com/files/GeoLiteCity.dat"), "./ip_geo"),
    (new URI("s3://private-bucket/files/GeoIPISP.dat"), "./ip_isp")
  )
}
