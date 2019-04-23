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
package com.snowplowanalytics.snowplow.enrich.common.enrichments.registry

import cats.Eval
import cats.syntax.option._
import cats.syntax.either._
import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer}
import com.snowplowanalytics.maxmind.iplookups.model.IpLocation
import io.circe.literal._
import org.specs2.Specification
import org.specs2.matcher.DataTables

class IpLookupsEnrichmentSpec extends Specification with DataTables {
  def is = s2"""
  This is a specification to test the IpLookupsEnrichment
  extractIpInformation should correctly extract location data from IP addresses where possible      $e1
  extractIpInformation should correctly extract ISP data from IP addresses where possible           $e2
  """

  // When testing, localMode is set to true, so the URIs are ignored and the databases are loaded from test/resources
  val config = IpLookupsEnrichment
    .parse(
      json"""{
      "name": "ip_lookups",
      "vendor": "com.snowplowanalytics.snowplow",
      "enabled": true,
      "parameters": {
        "geo": {
          "database": "GeoIP2-City.mmdb",
          "uri": "http://snowplow-hosted-assets.s3.amazonaws.com/third-party/maxmind"
        },
        "isp": {
          "database": "GeoIP2-ISP.mmdb",
          "uri": "http://snowplow-hosted-assets.s3.amazonaws.com/third-party/maxmind"
        }
      }
    }""",
      SchemaKey(
        "com.snowplowanalytics.snowplow",
        "ip_lookups",
        "jsonschema",
        SchemaVer.Full(2, 0, 0)
      ),
      true
    )
    .toOption
    .get

  def e1 =
    "SPEC NAME" || "IP ADDRESS" | "EXPECTED LOCATION" |
      "blank IP address" !! "" ! "AddressNotFoundException".asLeft.some |
      "null IP address" !! null ! "AddressNotFoundException".asLeft.some |
      "invalid IP address #1" !! "localhost" ! "AddressNotFoundException".asLeft.some |
      "invalid IP address #2" !! "hello" ! "UnknownHostException".asLeft.some |
      "valid IP address" !! "175.16.199.0" !
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
        ).asRight.some |
      "valid IP address with port" !! "175.16.199.0:8080" !
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
        ).asRight.some |> { (_, ipAddress, expected) =>
      (for {
        e <- config.enrichment[Eval]
        res <- e.extractIpInformation(ipAddress)
      } yield res.ipLocation).value.map(_.leftMap(_.getClass.getSimpleName)) must_== expected
    }

  def e2 =
    config.enrichment
      .extractIpInformation("70.46.123.145")
      .isp must_== "FDN Communications".asRight.some
}
