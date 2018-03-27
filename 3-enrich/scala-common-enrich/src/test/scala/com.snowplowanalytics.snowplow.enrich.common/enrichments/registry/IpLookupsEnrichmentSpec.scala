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
package com.snowplowanalytics
package snowplow.enrich.common
package enrichments
package registry

// Java
import java.net.URI

// Specs2, Scalaz-Specs2 & ScalaCheck
import org.specs2.{ScalaCheck, Specification}
import org.specs2.matcher.DataTables
import org.specs2.scalaz.ValidationMatchers
import org.scalacheck._
import org.scalacheck.Arbitrary._

// Scalaz
import scalaz._
import Scalaz._

// Scala MaxMind GeoIP
import maxmind.iplookups.IpLookups
import maxmind.iplookups.model.IpLocation

class IpLookupsEnrichmentSpec extends Specification with DataTables with ValidationMatchers with ScalaCheck {
  def is = s2"""
  This is a specification to test the IpLookupsEnrichment
  extractIpInformation should correctly extract location data from IP addresses where possible      $e1
  extractIpInformation should correctly extract ISP data from IP addresses where possible           $e2
  an IpLookupsEnrichment instance should expose no database files to cache in local mode            $e3
  an IpLookupsEnrichment instance should expose a list of database files to cache in non-local mode $e4
  """

  // When testing, localMode is set to true, so the URIs are ignored and the databases are loaded from test/resources
  val config = IpLookupsEnrichment(
    Some(("geo", new URI("/ignored-in-local-mode/"), "GeoIP2-City.mmdb")),
    Some(("isp", new URI("/ignored-in-local-mode/"), "GeoIP2-ISP.mmdb")),
    None,
    None,
    true
  )

  def e1 =
    "SPEC NAME"               || "IP ADDRESS" | "EXPECTED LOCATION" |
      "blank IP address"      !! "" ! Some(Failure("The address 127.0.0.1 is not in the database.")) |
      "null IP address"       !! null ! Some(Failure("The address 127.0.0.1 is not in the database.")) |
      "invalid IP address #1" !! "localhost" ! Some(Failure("The address 127.0.0.1 is not in the database.")) |
      "invalid IP address #2" !! "hello" ! Some(Failure("hello: Name or service not known")) |
      "valid IP address"      !! "175.16.199.0" !
        IpLocation( // Taken from scala-maxmind-geoip. See that test suite for other valid IP addresses
          countryCode = "CN",
          countryName = "China",
          region      = Some("22"),
          city        = Some("Changchun"),
          latitude    = 43.88F,
          longitude   = 125.3228F,
          timezone    = Some("Asia/Harbin"),
          postalCode  = None,
          metroCode   = None,
          regionName  = Some("Jilin Sheng")
        ).success.some |> { (_, ipAddress, expected) =>
      config.extractIpInformation(ipAddress).ipLocation.map(_.leftMap(_.getMessage)) must_== expected
    }

  def e2 = config.extractIpInformation("70.46.123.145").isp must_== "FDN Communications".success.some

  def e3 = config.dbsToCache must_== Nil

  val configRemote = IpLookupsEnrichment(
    Some(("geo", new URI("http://public-website.com/files/GeoLite2-City.mmdb"), "GeoLite2-City.mmdb")),
    Some(("isp", new URI("s3://private-bucket/files/GeoIP2-ISP.mmdb"), "GeoIP2-ISP.mmdb")),
    None,
    None,
    false
  )

  def e4 = configRemote.dbsToCache must_== List(
    (new URI("http://public-website.com/files/GeoLite2-City.mmdb"), "./ip_geo"),
    (new URI("s3://private-bucket/files/GeoIP2-ISP.mmdb"), "./ip_isp")
  )
}
